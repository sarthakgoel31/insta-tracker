"""FastAPI server for Social Branding Tracker — multi-tenant SaaS."""

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import base64
import json
import logging
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import (
    get_supabase, init_db, get_user_tier,
    list_reels as db_list_reels, get_reel, insert_reel, update_reel as db_update_reel,
    delete_reel as db_delete_reel, bulk_delete_reels as db_bulk_delete,
    insert_snapshot, get_latest_snapshot, get_prev_snapshot,
    get_monthly_views, upsert_monthly_views, upsert_monthly_views_auto,
    delete_future_auto_monthly, sum_monthly_views_before,
    get_monthly_view_entry, get_snapshots_in_range,
    list_columns as db_list_columns, insert_column as db_insert_column,
    delete_column as db_delete_column, list_months as db_list_months,
    get_reel_snapshots, get_all_reels_for_analytics, get_snapshots_for_reel,
)
from auth import AuthMiddleware, get_user_id, is_anonymous, require_auth, TRIAL_LIMIT
from scraper import (
    detect_platform,
    fetch_reel_data,
    ig_is_logged_in,
    ig_username,
)

# In-memory trial storage (per IP, wiped on restart — that's fine for trials)
_trial_data: dict[str, list[dict]] = {}  # IP → list of reel dicts
_trial_lock = threading.Lock()

logger = logging.getLogger("social-tracker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Auth middleware
app.add_middleware(AuthMiddleware)


from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest


class NoCacheAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        return response


app.add_middleware(NoCacheAPIMiddleware)

CAL_URL = "https://cal.com/sarthakgoel31"


@app.on_event("startup")
def startup():
    init_db()
    from pathlib import Path
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    # Load IG cookies from env var (for server-side scraping)
    ig_user = os.environ.get("IG_USERNAME")
    ig_cookies_b64 = os.environ.get("IG_COOKIES_B64")
    if ig_cookies_b64 and ig_user:
        cookies_file = data_dir / "ig_cookies.json"
        username_file = data_dir / "ig_username.txt"
        if not cookies_file.exists():
            cookies_file.write_bytes(base64.b64decode(ig_cookies_b64))
            username_file.write_text(ig_user)

    # Daily cron — 8 AM IST auto-refresh (all users)
    cron_thread = threading.Thread(target=_cron_loop, daemon=True)
    cron_thread.start()


@app.get("/")
def root():
    return FileResponse("static/index.html")


@app.get("/api/health")
def health():
    return {"status": "ok"}


# ── Models ──────────────────────────────────────────────────


class ReelCreate(BaseModel):
    url: str
    title: str = ""


class BulkAdd(BaseModel):
    urls: list[str]


class ReelUpdate(BaseModel):
    title: str | None = None
    posted_date: str | None = None
    custom_fields: dict | None = None


class ColumnCreate(BaseModel):
    name: str


class MonthlyViewEntry(BaseModel):
    month: str
    month_views: int


class BulkAction(BaseModel):
    ids: list[int]


# ── Status ─────────────────────────────────────────────────


@app.get("/api/status")
def status(request: Request):
    if is_anonymous(request):
        ip = request.client.host
        with _trial_lock:
            count = len(_trial_data.get(ip, []))
        return {
            "instagram": {"available": False, "locked": True, "locked_message": f"Instagram tracking requires a paid plan. Book a call: {CAL_URL}"},
            "youtube": {"available": True},
            "facebook": {"available": True},
            "tier": "trial",
            "trial_used": count,
            "trial_limit": TRIAL_LIMIT,
            "authenticated": False,
        }
    user_id = get_user_id(request)
    tier = get_user_tier(user_id)
    return {
        "instagram": {
            "available": tier == "paid",
            "locked": tier == "free",
            "locked_message": f"Instagram tracking is a paid feature. Book a call to enable: {CAL_URL}",
        },
        "youtube": {"available": True},
        "facebook": {"available": True},
        "tier": tier,
        "authenticated": True,
    }


# ── Months ─────────────────────────────────────────────────


@app.get("/api/months")
def api_list_months(request: Request):
    if is_anonymous(request):
        return {"months": []}
    user_id = get_user_id(request)
    return {"months": db_list_months(user_id)}


# ── Reels CRUD ──────────────────────────────────────────────


@app.get("/api/reels")
def api_list_reels(request: Request, month: str | None = None):
    if is_anonymous(request):
        ip = request.client.host
        with _trial_lock:
            trial_reels = _trial_data.get(ip, [])
        return {"reels": trial_reels, "trial": True, "trial_used": len(trial_reels), "trial_limit": TRIAL_LIMIT}
    user_id = get_user_id(request)
    tier = get_user_tier(user_id)
    reels = db_list_reels(user_id, month)
    result = []
    for r in reels:
        reel_id = r["id"]
        latest = get_latest_snapshot(reel_id)
        prev = get_prev_snapshot(reel_id)

        growth = None
        if latest and prev and latest.get("views") is not None and prev.get("views") is not None:
            growth = latest["views"] - prev["views"]

        mv_rows = get_monthly_views(reel_id)
        monthly_views_list = []
        mv_sum = 0
        for mv in mv_rows:
            views_val = mv.get("cumulative_views") or 0
            monthly_views_list.append({
                "month": mv["month"],
                "views": views_val,
                "is_manual": bool(mv.get("is_manual")),
            })
            mv_sum += views_val

        total_views = latest["views"] if latest and latest.get("views") is not None else 0
        current_month_auto = max(total_views - mv_sum, 0)

        # Instagram tier gating: show data but mark as locked
        is_ig = r.get("platform") == "instagram"
        ig_locked = is_ig and tier == "free"

        result.append({
            "id": reel_id,
            "url": r["url"],
            "title": r.get("title", ""),
            "posted_date": r.get("posted_date"),
            "platform": r.get("platform", "instagram"),
            "account": r.get("account", ""),
            "custom_fields": r.get("custom_fields", {}),
            "created_at": r.get("created_at"),
            "views": None if ig_locked else (latest["views"] if latest else None),
            "likes": None if ig_locked else (latest.get("likes") if latest else None),
            "comments": None if ig_locked else (latest.get("comments") if latest else None),
            "last_fetched": None if ig_locked else (latest.get("fetched_at") if latest else None),
            "growth": None if ig_locked else growth,
            "monthly_views": [] if ig_locked else monthly_views_list,
            "current_month_auto": 0 if ig_locked else current_month_auto,
            "ig_locked": ig_locked,
            "ig_locked_message": f"Instagram tracking requires a paid plan. Contact: {CAL_URL}" if ig_locked else None,
        })
    return {"reels": result}


@app.post("/api/reels")
def add_reel(request: Request, reel: ReelCreate):
    platform = detect_platform(reel.url)
    if platform == "unknown":
        raise HTTPException(400, "Unsupported URL. Use Instagram, YouTube, or Facebook links.")

    # Trial mode (anonymous)
    if is_anonymous(request):
        ip = request.client.host
        # Block Instagram for trial
        if platform == "instagram":
            raise HTTPException(403, f"Instagram tracking requires a paid plan. Sign up for free (YouTube + Facebook), or book a call for Instagram: {CAL_URL}")
        with _trial_lock:
            trial = _trial_data.setdefault(ip, [])
            if len(trial) >= TRIAL_LIMIT:
                raise HTTPException(403, f"Trial limit reached ({TRIAL_LIMIT} URLs). Sign up for free to track unlimited YouTube & Facebook URLs.")
            # Check duplicate
            if any(r["url"] == reel.url.strip() for r in trial):
                raise HTTPException(400, "URL already exists")
        # Scrape immediately for trial
        data = fetch_reel_data(reel.url.strip())
        trial_reel = {
            "id": len(trial) + 1,
            "url": reel.url.strip(),
            "title": data.get("title", reel.title.strip()),
            "posted_date": data.get("posted_date"),
            "platform": platform,
            "account": data.get("account", ""),
            "views": data.get("views") if "error" not in data else None,
            "likes": data.get("likes") if "error" not in data else None,
            "comments": data.get("comments") if "error" not in data else None,
            "last_fetched": datetime.now(timezone.utc).isoformat(),
            "growth": None,
            "monthly_views": [],
            "current_month_auto": 0,
            "custom_fields": {},
            "created_at": datetime.now(timezone.utc).isoformat(),
            "ig_locked": False,
            "ig_locked_message": None,
            "error": data.get("error"),
        }
        with _trial_lock:
            trial.append(trial_reel)
        return {"ok": True, "trial_used": len(trial), "trial_limit": TRIAL_LIMIT}

    user_id = get_user_id(request)
    try:
        insert_reel(user_id, reel.url, reel.title, platform)
    except Exception:
        raise HTTPException(400, "URL already exists")
    return {"ok": True}


@app.post("/api/reels/bulk")
def add_reels_bulk(request: Request, data: BulkAdd):
    # Trial mode: process up to TRIAL_LIMIT, skip rest
    if is_anonymous(request):
        ip = request.client.host
        added, skipped_list = 0, []
        for url in data.urls:
            url = url.strip()
            if not url:
                continue
            platform = detect_platform(url)
            if platform == "unknown":
                skipped_list.append({"url": url, "reason": "Unsupported platform"})
                continue
            if platform == "instagram":
                skipped_list.append({"url": url, "reason": f"Instagram requires paid plan. Book a call: {CAL_URL}"})
                continue
            with _trial_lock:
                trial = _trial_data.setdefault(ip, [])
                if len(trial) >= TRIAL_LIMIT:
                    skipped_list.append({"url": url, "reason": f"Trial limit ({TRIAL_LIMIT} URLs). Sign up free for unlimited."})
                    continue
                if any(r["url"] == url for r in trial):
                    skipped_list.append({"url": url, "reason": "Duplicate URL"})
                    continue
            result = fetch_reel_data(url)
            trial_reel = {
                "id": len(trial) + 1, "url": url, "title": result.get("title", ""),
                "posted_date": result.get("posted_date"), "platform": platform,
                "account": result.get("account", ""),
                "views": result.get("views") if "error" not in result else None,
                "likes": result.get("likes") if "error" not in result else None,
                "comments": result.get("comments") if "error" not in result else None,
                "last_fetched": datetime.now(timezone.utc).isoformat(),
                "growth": None, "monthly_views": [], "current_month_auto": 0,
                "custom_fields": {}, "created_at": datetime.now(timezone.utc).isoformat(),
                "ig_locked": False, "ig_locked_message": None,
            }
            with _trial_lock:
                trial.append(trial_reel)
            added += 1
        with _trial_lock:
            count = len(_trial_data.get(ip, []))
        return {"added": added, "skipped": len(skipped_list), "skipped_details": skipped_list,
                "trial_used": count, "trial_limit": TRIAL_LIMIT}

    user_id = get_user_id(request)
    added, skipped_list = 0, []
    for url in data.urls:
        url = url.strip()
        if not url:
            continue
        platform = detect_platform(url)
        if platform == "unknown":
            skipped_list.append({"url": url, "reason": "Unsupported platform"})
            continue
        try:
            insert_reel(user_id, url, "", platform)
            added += 1
        except Exception:
            skipped_list.append({"url": url, "reason": "Duplicate URL"})
    return {"added": added, "skipped": len(skipped_list), "skipped_details": skipped_list}


@app.put("/api/reels/{reel_id}")
def update_reel_route(request: Request, reel_id: int, data: ReelUpdate):
    user_id = require_auth(request)
    reel = get_reel(user_id, reel_id)
    if not reel:
        raise HTTPException(404, "Reel not found")

    updates = {}
    if data.title is not None:
        updates["title"] = data.title
    if data.posted_date is not None:
        updates["posted_date"] = data.posted_date
    if data.custom_fields is not None:
        existing = reel.get("custom_fields", {})
        if isinstance(existing, str):
            existing = json.loads(existing)
        existing.update(data.custom_fields)
        updates["custom_fields"] = existing

    if updates:
        db_update_reel(user_id, reel_id, updates)
    return {"ok": True}


@app.delete("/api/reels/{reel_id}")
def delete_reel_route(request: Request, reel_id: int):
    user_id = require_auth(request)
    db_delete_reel(user_id, reel_id)
    return {"ok": True}


@app.post("/api/reels/bulk-delete")
def bulk_delete_reels_route(request: Request, data: BulkAction):
    user_id = require_auth(request)
    deleted = db_bulk_delete(user_id, data.ids)
    return {"ok": True, "deleted": deleted}


@app.post("/api/reels/{reel_id}/refresh")
def refresh_single_reel(request: Request, reel_id: int):
    user_id = require_auth(request)
    reel = get_reel(user_id, reel_id)
    if not reel:
        raise HTTPException(404, "Reel not found")

    # Block IG refresh for free users
    tier = get_user_tier(user_id)
    if reel.get("platform") == "instagram" and tier == "free":
        raise HTTPException(403, f"Instagram tracking requires a paid plan. Book a call: {CAL_URL}")

    _process_single_reel(user_id, {"id": reel_id, "url": reel["url"], "posted_date": reel.get("posted_date")})
    return {"ok": True}


@app.post("/api/reels/refresh-selected")
def refresh_selected_reels(request: Request, data: BulkAction):
    user_id = require_auth(request)
    tier = get_user_tier(user_id)

    reel_rows = []
    for rid in data.ids:
        reel = get_reel(user_id, rid)
        if reel:
            # Skip IG reels for free users
            if reel.get("platform") == "instagram" and tier == "free":
                continue
            reel_rows.append({"id": reel["id"], "url": reel["url"], "posted_date": reel.get("posted_date"), "platform": reel.get("platform")})

    if not reel_rows:
        raise HTTPException(400, "No valid reels to refresh (Instagram reels require a paid plan)")

    with _refresh_lock:
        if _refresh_state["running"]:
            raise HTTPException(409, "Refresh already in progress")
        _refresh_state["running"] = True
        _refresh_state["total"] = len(reel_rows)
        _refresh_state["completed"] = 0
        _refresh_state["errors"] = 0
        _refresh_state["error_details"] = []
        _refresh_state["current_url"] = ""
        _refresh_state["crosscheck"] = []

    thread = threading.Thread(target=_refresh_worker, args=(user_id, reel_rows), daemon=True)
    thread.start()
    return {"started": True, "total": len(reel_rows)}


@app.put("/api/reels/{reel_id}/override-views")
def override_views(request: Request, reel_id: int, data: dict):
    user_id = require_auth(request)
    reel = get_reel(user_id, reel_id)
    if not reel:
        raise HTTPException(404, "Reel not found")
    views = data.get("views")
    if views is None:
        raise HTTPException(400, "views required")
    # Update latest snapshot by inserting a new one with overridden views
    latest = get_latest_snapshot(reel_id)
    insert_snapshot(user_id, reel_id, views, latest.get("likes") if latest else None, latest.get("comments") if latest else None)
    return {"ok": True}


@app.put("/api/reels/{reel_id}/monthly-views")
def set_monthly_views(request: Request, reel_id: int, data: MonthlyViewEntry):
    user_id = require_auth(request)
    reel = get_reel(user_id, reel_id)
    if not reel:
        raise HTTPException(404, "Reel not found")
    upsert_monthly_views(user_id, reel_id, data.month, data.month_views, is_manual=True)
    delete_future_auto_monthly(reel_id, data.month)
    return {"ok": True}


# ── Custom Columns ──────────────────────────────────────────


@app.get("/api/columns")
def list_columns_route(request: Request):
    if is_anonymous(request):
        return {"columns": []}
    user_id = get_user_id(request)
    return {"columns": db_list_columns(user_id)}


@app.post("/api/columns")
def add_column(request: Request, data: ColumnCreate):
    user_id = require_auth(request)
    if not db_insert_column(user_id, data.name):
        raise HTTPException(400, "Column already exists")
    return {"ok": True}


@app.delete("/api/columns/{col_id}")
def delete_column_route(request: Request, col_id: int):
    user_id = require_auth(request)
    db_delete_column(user_id, col_id)
    return {"ok": True}


# ── Refresh Views (background with progress) ───────────────

DELAY_IG = 0.5
DELAY_FB = 2
YT_WORKERS = 3
IG_WORKERS = 3

_refresh_state = {
    "running": False,
    "total": 0,
    "completed": 0,
    "errors": 0,
    "current_url": "",
    "error_details": [],
}
_last_refresh_result = {
    "completed_at": None,
    "total": 0,
    "errors": 0,
    "error_details": [],
    "crosscheck": [],
}
_refresh_lock = threading.Lock()


def _process_single_reel(user_id: str, reel: dict) -> None:
    """Fetch data for a single reel and persist to Supabase. Thread-safe."""
    reel_id, url, existing_posted = reel["id"], reel["url"], reel.get("posted_date")

    with _refresh_lock:
        _refresh_state["current_url"] = url

    data = fetch_reel_data(url)

    if "error" in data:
        logger.warning("Refresh error for %s: %s", url, data["error"])
        with _refresh_lock:
            _refresh_state["completed"] += 1
            _refresh_state["errors"] += 1
            _refresh_state["error_details"].append({"url": url, "error": data["error"]})
        return

    source = data.pop("_source", "unknown")
    if source in ("instaloader", "embed") and "instagram.com" in url:
        with _refresh_lock:
            _refresh_state.setdefault("crosscheck", []).append({
                "url": url, "views": data.get("views"), "source": source,
                "account": data.get("account", ""),
            })

    # Insert snapshot
    insert_snapshot(user_id, reel_id, data.get("views"), data.get("likes"), data.get("comments"))

    # Update reel metadata if missing
    updates = {}
    if not existing_posted and data.get("posted_date"):
        updates["posted_date"] = data["posted_date"]
    if data.get("title"):
        reel_data = get_reel(user_id, reel_id)
        if reel_data and not reel_data.get("title"):
            updates["title"] = data["title"]
    if data.get("account"):
        reel_data = get_reel(user_id, reel_id)
        if reel_data and not reel_data.get("account"):
            updates["account"] = data["account"]
    if updates:
        db_update_reel(user_id, reel_id, updates)

    # Auto-populate monthly_views
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    total_views = data.get("views")
    if total_views is not None:
        # Month-end backfill
        prev_month_dt = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
        prev_month = prev_month_dt.strftime("%Y-%m")
        prev_entry = get_monthly_view_entry(reel_id, prev_month)
        if not prev_entry:
            prev_month_end = prev_month_dt.strftime("%Y-%m-%dT23:59:59+00:00")
            prev_month_start = prev_month + "-01T00:00:00+00:00"
            prev_snaps = get_snapshots_in_range(reel_id, prev_month_start, prev_month_end)
            if prev_snaps and prev_snaps[0].get("views") is not None:
                older_sum = sum_monthly_views_before(reel_id, prev_month)
                prev_month_views = max(prev_snaps[0]["views"] - older_sum, 0)
                if prev_month_views > 0:
                    # Insert only if not exists
                    if not get_monthly_view_entry(reel_id, prev_month):
                        upsert_monthly_views(user_id, reel_id, prev_month, prev_month_views)

        # Current month calculation
        existing = get_monthly_view_entry(reel_id, current_month)
        if not existing or not existing.get("is_manual"):
            previous_months_sum = sum_monthly_views_before(reel_id, current_month)
            current_month_views = max(total_views - previous_months_sum, 0)
            upsert_monthly_views_auto(user_id, reel_id, current_month, current_month_views)

    with _refresh_lock:
        _refresh_state["completed"] += 1


def _process_ig(user_id: str, reels: list) -> None:
    with ThreadPoolExecutor(max_workers=IG_WORKERS) as pool:
        futures = {pool.submit(_process_single_reel, user_id, reel): reel for reel in reels}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.exception("IG worker error for %s", futures[future]["url"])
            time.sleep(DELAY_IG)


def _process_youtube(user_id: str, reels: list) -> None:
    with ThreadPoolExecutor(max_workers=YT_WORKERS) as pool:
        futures = {pool.submit(_process_single_reel, user_id, reel): reel for reel in reels}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.exception("YouTube worker error for %s", futures[future]["url"])


def _process_fb(user_id: str, reels: list) -> None:
    for i, reel in enumerate(reels):
        _process_single_reel(user_id, reel)
        if i < len(reels) - 1:
            time.sleep(DELAY_FB)


def _run_platform_threads(user_id: str, by_platform: dict[str, list]):
    platform_handlers = {
        "instagram": _process_ig,
        "youtube": _process_youtube,
        "facebook": _process_fb,
    }
    threads: list[threading.Thread] = []
    for platform, reels in by_platform.items():
        handler = platform_handlers.get(platform, _process_ig)
        t = threading.Thread(target=handler, args=(user_id, reels), daemon=True)
        t.name = f"refresh-{platform}"
        threads.append(t)
        t.start()
    for t in threads:
        t.join()


def _refresh_worker(user_id: str, reel_rows: list):
    by_platform: dict[str, list] = defaultdict(list)
    for reel in reel_rows:
        by_platform[reel.get("platform", "instagram")].append(reel)

    _run_platform_threads(user_id, by_platform)

    with _refresh_lock:
        _refresh_state["running"] = False
        _refresh_state["current_url"] = ""
        _last_refresh_result["completed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _last_refresh_result["total"] = _refresh_state["total"]
        _last_refresh_result["errors"] = _refresh_state["errors"]
        _last_refresh_result["error_details"] = list(_refresh_state["error_details"])
        _last_refresh_result["crosscheck"] = list(_refresh_state.get("crosscheck", []))


@app.post("/api/refresh")
def refresh_views(request: Request):
    user_id = require_auth(request)
    tier = get_user_tier(user_id)

    with _refresh_lock:
        if _refresh_state["running"]:
            raise HTTPException(409, "Refresh already in progress")

    reels = db_list_reels(user_id)
    if not reels:
        return {"total": 0, "message": "No reels to refresh"}

    # Filter out IG reels for free users
    reel_rows = []
    ig_skipped = 0
    for r in reels:
        if r.get("platform") == "instagram" and tier == "free":
            ig_skipped += 1
            continue
        reel_rows.append({"id": r["id"], "url": r["url"], "posted_date": r.get("posted_date"), "platform": r.get("platform")})

    if not reel_rows:
        return {"total": 0, "message": f"No reels to refresh ({ig_skipped} Instagram reels skipped — paid plan required)"}

    with _refresh_lock:
        _refresh_state["running"] = True
        _refresh_state["total"] = len(reel_rows)
        _refresh_state["completed"] = 0
        _refresh_state["errors"] = 0
        _refresh_state["error_details"] = []
        _refresh_state["current_url"] = ""
        _refresh_state["crosscheck"] = []

    thread = threading.Thread(target=_refresh_worker, args=(user_id, reel_rows), daemon=True)
    thread.start()

    return {"started": True, "total": len(reel_rows), "ig_skipped": ig_skipped}


@app.get("/api/refresh/progress")
def refresh_progress():
    with _refresh_lock:
        return dict(_refresh_state)


@app.get("/api/refresh/last")
def last_refresh():
    return dict(_last_refresh_result)


@app.post("/api/refresh/reset")
def refresh_reset():
    with _refresh_lock:
        _refresh_state["running"] = False
        _refresh_state["current_url"] = ""
    return {"ok": True}


# ── Daily Cron ─────────────────────────────────────────────

CRON_HOUR_UTC = 2
CRON_MINUTE_UTC = 30


def _seconds_until_next_cron() -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=CRON_HOUR_UTC, minute=CRON_MINUTE_UTC, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _cron_loop():
    """Daily refresh for ALL users' non-IG reels (and paid users' IG reels)."""
    while True:
        wait = _seconds_until_next_cron()
        logger.info("Cron: sleeping %.0f seconds until next run", wait)
        time.sleep(wait)
        logger.info("Cron: 8:00 AM IST — starting daily refresh")

        sb = get_supabase()
        # Get all users
        profiles = sb.table("tracker_profiles").select("id, tier").execute()
        for profile in profiles.data:
            uid = profile["id"]
            tier = profile.get("tier", "free")
            reels = db_list_reels(uid)
            reel_rows = []
            for r in reels:
                if r.get("platform") == "instagram" and tier == "free":
                    continue
                reel_rows.append({"id": r["id"], "url": r["url"], "posted_date": r.get("posted_date"), "platform": r.get("platform")})
            if reel_rows:
                logger.info("Cron: refreshing %d reels for user %s", len(reel_rows), uid[:8])
                _refresh_worker(uid, reel_rows)

        time.sleep(61)


# ── Snapshots ──────────────────────────────────────────────


@app.get("/api/reels/{reel_id}/snapshots")
def reel_snapshots(request: Request, reel_id: int):
    user_id = require_auth(request)
    reel = get_reel(user_id, reel_id)
    if not reel:
        raise HTTPException(404, "Reel not found")
    return {"snapshots": get_reel_snapshots(reel_id)}


# ── Analytics ──────────────────────────────────────────────


def _month_diff(ym1: str, ym2: str) -> int:
    y1, m1 = int(ym1[:4]), int(ym1[5:7])
    y2, m2 = int(ym2[:4]), int(ym2[5:7])
    return (y1 - y2) * 12 + (m1 - m2)


def _monthly_gains_for_reel(reel_id: int, posted_date: str | None = None) -> dict[str, int]:
    rows = get_snapshots_for_reel(reel_id)
    if not rows:
        return {}
    month_last: dict[str, int] = {}
    for r in rows:
        fetched = r.get("fetched_at", "")
        if isinstance(fetched, str) and len(fetched) >= 7:
            month_last[fetched[:7]] = r["views"]
    months = sorted(month_last.keys())
    gains = {}
    for i, m in enumerate(months):
        if i == 0:
            target_month = posted_date[:7] if posted_date and len(posted_date) >= 7 else m
            gains[target_month] = gains.get(target_month, 0) + month_last[m]
        else:
            gains[m] = gains.get(m, 0) + month_last[m] - month_last[months[i - 1]]
    return gains


@app.get("/api/analytics/monthly")
def monthly_analytics(request: Request):
    if is_anonymous(request):
        return {"months": []}
    user_id = get_user_id(request)
    reels = get_all_reels_for_analytics(user_id)
    totals: dict[str, int] = {}
    for reel in reels:
        for month, gain in _monthly_gains_for_reel(reel["id"], reel.get("posted_date")).items():
            totals[month] = totals.get(month, 0) + max(gain, 0)
    return {"months": [{"month": m, "views": v} for m, v in sorted(totals.items(), reverse=True)]}


@app.get("/api/analytics/cohort-summary")
def cohort_summary(request: Request):
    if is_anonymous(request):
        return {"summary": []}
    user_id = get_user_id(request)
    reels = get_all_reels_for_analytics(user_id)
    month_cohorts: dict[str, dict[str, int]] = {}
    month_accounts: dict[str, dict[str, int]] = {}

    for reel in reels:
        posted = reel.get("posted_date")
        if not posted or len(posted) < 7:
            continue
        posted_ym = posted[:7]
        account = reel.get("account") or "Unknown"

        mv_rows = get_monthly_views(reel["id"])
        if mv_rows:
            for mv in mv_rows:
                delta = mv.get("cumulative_views") or 0
                if delta <= 0:
                    continue
                cal_month = mv["month"]
                age = _month_diff(cal_month, posted_ym)
                if age < 0:
                    continue
                label = f"M{age}"
                month_cohorts.setdefault(cal_month, {})
                month_cohorts[cal_month][label] = month_cohorts[cal_month].get(label, 0) + delta
                month_accounts.setdefault(cal_month, {})
                month_accounts[cal_month][account] = month_accounts[cal_month].get(account, 0) + delta
        else:
            gains = _monthly_gains_for_reel(reel["id"], posted)
            for cal_month, gain in gains.items():
                if gain <= 0:
                    continue
                age = _month_diff(cal_month, posted_ym)
                if age < 0:
                    continue
                label = f"M{age}"
                month_cohorts.setdefault(cal_month, {})
                month_cohorts[cal_month][label] = month_cohorts[cal_month].get(label, 0) + gain
                month_accounts.setdefault(cal_month, {})
                month_accounts[cal_month][account] = month_accounts[cal_month].get(account, 0) + gain

    summary = []
    for m in sorted(month_cohorts.keys(), reverse=True):
        cohorts = month_cohorts[m]
        total = sum(cohorts.values())
        by_account = month_accounts.get(m, {})
        summary.append({"month": m, "cohorts": cohorts, "total": total, "by_account": by_account})
    return {"summary": summary}


@app.get("/api/analytics/pivot")
def pivot_analytics(request: Request, group_by: str = "account", ids: str | None = None):
    if is_anonymous(request):
        return {"months": [], "rows": [], "totals": {"months": {}, "total": 0}}
    user_id = get_user_id(request)
    if group_by not in ("account", "platform", "month"):
        raise HTTPException(400, "group_by must be 'account', 'platform', or 'month'")

    id_filter = None
    if ids:
        id_filter = [int(x) for x in ids.split(",") if x.strip().isdigit()]

    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    reels = get_all_reels_for_analytics(user_id)
    if id_filter:
        reels = [r for r in reels if r["id"] in id_filter]

    if group_by == "month":
        month_cohorts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        for reel in reels:
            posted = reel.get("posted_date")
            if not posted or len(posted) < 7:
                continue
            posted_ym = posted[:7]
            mv_rows = get_monthly_views(reel["id"])
            mv_sum = 0
            for mv in mv_rows:
                views = mv.get("cumulative_views") or 0
                if views > 0:
                    age = _month_diff(mv["month"], posted_ym)
                    if age >= 0:
                        month_cohorts[mv["month"]][age] += views
                    mv_sum += views
            latest = get_latest_snapshot(reel["id"])
            total_views = latest["views"] if latest and latest.get("views") is not None else 0
            auto_val = max(total_views - mv_sum, 0)
            if auto_val > 0:
                age = _month_diff(current_month, posted_ym)
                if age >= 0:
                    month_cohorts[current_month][age] += auto_val

        max_age = 0
        for cohorts in month_cohorts.values():
            if cohorts:
                max_age = max(max_age, max(cohorts.keys()))

        cols = [f"M{i}" for i in range(max_age + 1)]
        rows = []
        column_totals: dict[str, int] = defaultdict(int)
        grand_total = 0
        for month in sorted(month_cohorts.keys(), reverse=True):
            cohorts = month_cohorts[month]
            row_total = sum(cohorts.values())
            grand_total += row_total
            months_dict = {}
            for age, views in cohorts.items():
                key = f"M{age}"
                months_dict[key] = views
                column_totals[key] += views
            rows.append({"label": month, "months": months_dict, "total": row_total})

        return {"months": cols, "rows": rows, "totals": {"months": dict(column_totals), "total": grand_total}}

    # account / platform grouping
    row_data: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for reel in reels:
        label = (reel.get("account") or "Unknown") if group_by == "account" else (reel.get("platform") or "Unknown")
        mv_rows = get_monthly_views(reel["id"])
        mv_sum = 0
        for mv in mv_rows:
            views = mv.get("cumulative_views") or 0
            if views > 0:
                row_data[label][mv["month"]] += views
                mv_sum += views
        latest = get_latest_snapshot(reel["id"])
        total_views = latest["views"] if latest and latest.get("views") is not None else 0
        auto_val = max(total_views - mv_sum, 0)
        if auto_val > 0:
            row_data[label][current_month] += auto_val

    all_months: set[str] = set()
    for months_dict in row_data.values():
        all_months.update(months_dict.keys())

    rows = []
    column_totals = defaultdict(int)
    grand_total = 0
    for label in sorted(row_data.keys()):
        months_dict = row_data[label]
        row_total = sum(months_dict.values())
        grand_total += row_total
        for m, v in months_dict.items():
            column_totals[m] += v
        rows.append({"label": label, "months": dict(months_dict), "total": row_total})

    return {
        "months": sorted(all_months),
        "rows": rows,
        "totals": {"months": dict(sorted(column_totals.items())), "total": grand_total},
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8501))
    uvicorn.run(app, host="0.0.0.0", port=port)
