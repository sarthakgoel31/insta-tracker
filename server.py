"""FastAPI server for MBB Social Branding Tracker."""

import base64
import json
import logging
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import get_db, init_db
from scraper import (
    detect_platform,
    fetch_reel_data,
    ig_2fa,
    ig_is_logged_in,
    ig_login,
    ig_logout,
    ig_username,
)

logger = logging.getLogger("insta-tracker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class NoCacheAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        return response


app.add_middleware(NoCacheAPIMiddleware)


@app.on_event("startup")
def startup():
    init_db()
    from pathlib import Path
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    ig_user = os.environ.get("IG_USERNAME")

    # Load IG cookies from env var (Playwright format, preferred)
    ig_cookies_b64 = os.environ.get("IG_COOKIES_B64")
    if ig_cookies_b64 and ig_user:
        cookies_file = data_dir / "ig_cookies.json"
        username_file = data_dir / "ig_username.txt"
        if not cookies_file.exists():
            cookies_file.write_bytes(base64.b64decode(ig_cookies_b64))
            username_file.write_text(ig_user)

    # Legacy: load instaloader session + auto-export cookies
    ig_session_b64 = os.environ.get("IG_SESSION")
    if ig_session_b64 and ig_user:
        session_file = data_dir / "ig_session"
        username_file = data_dir / "ig_username.txt"
        if not session_file.exists():
            session_file.write_bytes(base64.b64decode(ig_session_b64))
            username_file.write_text(ig_user)
        # Export instaloader cookies to JSON for Playwright/GraphQL
        cookies_file = data_dir / "ig_cookies.json"
        if not cookies_file.exists():
            try:
                from scraper import _get_ig_loader, _export_cookies_to_json
                L = _get_ig_loader()
                if L.context.is_logged_in:
                    _export_cookies_to_json(L)
            except Exception:
                pass

    # Start daily cron refresh thread
    cron_thread = threading.Thread(target=_cron_loop, daemon=True)
    cron_thread.start()


@app.get("/")
def root():
    return FileResponse("static/index.html")


@app.get("/api/test-scrape")
def test_scrape(url: str):
    """Debug endpoint: test scraping a single URL and return raw result."""
    from scraper import fetch_reel_data, _ig_instaloader_last_error
    result = fetch_reel_data(url)
    if "error" in result and _ig_instaloader_last_error:
        result["instaloader_error"] = _ig_instaloader_last_error
    return result


@app.get("/api/debug")
def debug():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM reels").fetchone()[0]
    cols = [r[1] for r in conn.execute("PRAGMA table_info(reels)").fetchall()]
    rows = conn.execute("SELECT id, url, platform FROM reels").fetchall()
    conn.close()
    return {"count": count, "columns": cols, "rows": [dict(r) for r in rows]}


@app.get("/api/debug/ig-cookies")
def debug_ig_cookies():
    """Check IG cookie status without exposing values."""
    from scraper import _get_ig_cookies_list, _get_ig_cookies_dict, IG_COOKIES_FILE
    cookie_list = _get_ig_cookies_list()
    cookie_dict = _get_ig_cookies_dict()
    has_env = bool(os.environ.get("IG_COOKIES_B64"))
    has_file = IG_COOKIES_FILE.exists()
    # Also check instaloader session
    from scraper import _get_ig_loader, SESSION_FILE
    has_session_file = SESSION_FILE.exists()
    has_session_env = bool(os.environ.get("IG_SESSION_B64"))
    loader_logged_in = False
    try:
        L = _get_ig_loader()
        loader_logged_in = L.context.is_logged_in
    except Exception as e:
        loader_logged_in = f"error: {e}"

    return {
        "source": "env" if has_env else ("file" if has_file else "none"),
        "cookie_count": len(cookie_list),
        "has_sessionid": bool(cookie_dict.get("sessionid")),
        "has_csrftoken": bool(cookie_dict.get("csrftoken")),
        "cookie_names": list(cookie_dict.keys()),
        "instaloader_session_file": has_session_file,
        "instaloader_session_env": has_session_env,
        "instaloader_logged_in": loader_logged_in,
    }


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


class SheetExport(BaseModel):
    sheet_url: str


class BulkAction(BaseModel):
    ids: list[int]


class IGLoginReq(BaseModel):
    username: str
    password: str


class IGCodeReq(BaseModel):
    code: str


# ── Auth ────────────────────────────────────────────────────


@app.get("/api/status")
def status():
    return {
        "instagram": {"logged_in": ig_is_logged_in(), "username": ig_username()},
        "youtube": {"available": True},
        "facebook": {"available": True},
    }


@app.post("/api/auth/instagram")
def auth_instagram(data: IGLoginReq):
    return ig_login(data.username, data.password)


@app.post("/api/auth/instagram/2fa")
def auth_instagram_2fa(data: IGCodeReq):
    return ig_2fa(data.code)


@app.post("/api/auth/instagram/logout")
def auth_instagram_logout():
    ig_logout()
    return {"ok": True}


# ── Months ───────────────────────────────────────────────────


@app.get("/api/months")
def list_months():
    conn = get_db()
    reel_months = conn.execute(
        "SELECT DISTINCT substr(posted_date, 1, 7) AS month FROM reels "
        "WHERE posted_date IS NOT NULL AND posted_date != ''"
    ).fetchall()
    mv_months = conn.execute(
        "SELECT DISTINCT month FROM monthly_views WHERE month IS NOT NULL AND month != ''"
    ).fetchall()
    conn.close()
    all_months = sorted(
        set(
            [r["month"] for r in reel_months if r["month"]]
            + [r["month"] for r in mv_months if r["month"]]
        ),
        reverse=True,
    )
    return {"months": all_months}


# ── Reels CRUD ──────────────────────────────────────────────


@app.get("/api/reels")
def list_reels(month: str | None = None):
    conn = get_db()
    if month and month != "all":
        reels = conn.execute(
            "SELECT * FROM reels WHERE substr(posted_date, 1, 7) = ? ORDER BY created_at DESC",
            (month,),
        ).fetchall()
    else:
        reels = conn.execute("SELECT * FROM reels ORDER BY created_at DESC").fetchall()
    result = []
    for r in reels:
        latest = conn.execute(
            "SELECT views, likes, comments, fetched_at FROM snapshots "
            "WHERE reel_id = ? ORDER BY fetched_at DESC LIMIT 1",
            (r["id"],),
        ).fetchone()

        prev = conn.execute(
            "SELECT views FROM snapshots WHERE reel_id = ? ORDER BY fetched_at DESC LIMIT 1 OFFSET 1",
            (r["id"],),
        ).fetchone()

        growth = None
        if latest and prev and latest["views"] is not None and prev["views"] is not None:
            growth = latest["views"] - prev["views"]

        # Build monthly_views — values are already deltas
        mv_rows = conn.execute(
            "SELECT month, cumulative_views, is_manual FROM monthly_views "
            "WHERE reel_id = ? ORDER BY month",
            (r["id"],),
        ).fetchall()
        monthly_views_list = []
        mv_sum = 0
        for mv in mv_rows:
            views_val = mv["cumulative_views"] or 0
            monthly_views_list.append({
                "month": mv["month"],
                "views": views_val,
                "is_manual": bool(mv["is_manual"]),
            })
            mv_sum += views_val

        # current_month_auto: total views minus sum of all monthly entries
        total_views = latest["views"] if latest and latest["views"] is not None else 0
        current_month_auto = max(total_views - mv_sum, 0)

        result.append({
            "id": r["id"],
            "url": r["url"],
            "title": r["title"],
            "posted_date": r["posted_date"],
            "platform": r["platform"],
            "account": r["account"],
            "custom_fields": json.loads(r["custom_fields"]),
            "created_at": r["created_at"],
            "views": latest["views"] if latest else None,
            "likes": latest["likes"] if latest else None,
            "comments": latest["comments"] if latest else None,
            "last_fetched": latest["fetched_at"] if latest else None,
            "growth": growth,
            "monthly_views": monthly_views_list,
            "current_month_auto": current_month_auto,
        })
    conn.close()
    return {"reels": result}


@app.post("/api/reels")
def add_reel(reel: ReelCreate):
    platform = detect_platform(reel.url)
    if platform == "unknown":
        raise HTTPException(400, "Unsupported URL. Use Instagram, YouTube, or Facebook links.")
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO reels (url, title, platform) VALUES (?, ?, ?)",
            (reel.url.strip(), reel.title.strip(), platform),
        )
        conn.commit()
    except Exception:
        conn.close()
        raise HTTPException(400, "URL already exists")
    conn.close()
    return {"ok": True}


@app.post("/api/reels/bulk")
def add_reels_bulk(data: BulkAdd):
    conn = get_db()
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
            conn.execute("INSERT INTO reels (url, platform) VALUES (?, ?)", (url, platform))
            added += 1
        except Exception:
            skipped_list.append({"url": url, "reason": "Duplicate URL"})
    conn.commit()
    conn.close()
    return {"added": added, "skipped": len(skipped_list), "skipped_details": skipped_list}


@app.put("/api/reels/{reel_id}")
def update_reel(reel_id: int, data: ReelUpdate):
    conn = get_db()
    reel = conn.execute("SELECT * FROM reels WHERE id = ?", (reel_id,)).fetchone()
    if not reel:
        conn.close()
        raise HTTPException(404, "Reel not found")

    if data.title is not None:
        conn.execute("UPDATE reels SET title = ? WHERE id = ?", (data.title, reel_id))
    if data.posted_date is not None:
        conn.execute("UPDATE reels SET posted_date = ? WHERE id = ?", (data.posted_date, reel_id))
    if data.custom_fields is not None:
        existing = json.loads(reel["custom_fields"])
        existing.update(data.custom_fields)
        conn.execute("UPDATE reels SET custom_fields = ? WHERE id = ?", (json.dumps(existing), reel_id))

    conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/api/reels/{reel_id}")
def delete_reel(reel_id: int):
    conn = get_db()
    conn.execute("DELETE FROM reels WHERE id = ?", (reel_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/reels/bulk-delete")
def bulk_delete_reels(data: BulkAction):
    conn = get_db()
    for rid in data.ids:
        conn.execute("DELETE FROM reels WHERE id = ?", (rid,))
    conn.commit()
    conn.close()
    return {"ok": True, "deleted": len(data.ids)}


@app.post("/api/reels/{reel_id}/refresh")
def refresh_single_reel(reel_id: int):
    """Refresh a single reel's views."""
    conn = get_db()
    row = conn.execute("SELECT url, posted_date FROM reels WHERE id = ?", (reel_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Reel not found")
    reel = {"id": reel_id, "url": row["url"], "posted_date": row["posted_date"]}
    conn.close()
    _process_single_reel(reel)
    return {"ok": True}


@app.post("/api/reels/refresh-selected")
def refresh_selected_reels(data: BulkAction):
    """Refresh selected reels in background."""
    conn = get_db()
    reel_rows = []
    for rid in data.ids:
        row = conn.execute("SELECT id, url, posted_date FROM reels WHERE id = ?", (rid,)).fetchone()
        if row:
            reel_rows.append(dict(row))
    conn.close()
    if not reel_rows:
        raise HTTPException(400, "No valid reels")

    with _refresh_lock:
        if _refresh_state["running"]:
            raise HTTPException(409, "Refresh already in progress")
        _refresh_state["running"] = True
        _refresh_state["total"] = len(reel_rows)
        _refresh_state["completed"] = 0
        _refresh_state["errors"] = 0
        _refresh_state["error_details"] = []
        _refresh_state["current_url"] = ""
        _refresh_state["cookie_auto_refreshed"] = None
        _refresh_state["cookie_retry_recovered"] = 0
        _refresh_state["cookie_refresh_error"] = ""

    thread = threading.Thread(target=_refresh_worker, args=(reel_rows,), daemon=True)
    thread.start()
    return {"started": True, "total": len(reel_rows)}


@app.put("/api/reels/{reel_id}/monthly-views")
def set_monthly_views(reel_id: int, data: MonthlyViewEntry):
    conn = get_db()
    conn.execute(
        "INSERT INTO monthly_views (reel_id, month, cumulative_views, is_manual) "
        "VALUES (?, ?, ?, 1) "
        "ON CONFLICT(reel_id, month) DO UPDATE SET cumulative_views=excluded.cumulative_views, "
        "is_manual=1, updated_at=datetime('now')",
        (reel_id, data.month, data.month_views),
    )
    # Delete all future auto-entries so they recalculate correctly
    conn.execute(
        "DELETE FROM monthly_views WHERE reel_id = ? AND month > ? AND is_manual = 0",
        (reel_id, data.month),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


# ── Custom Columns ──────────────────────────────────────────


@app.get("/api/columns")
def list_columns():
    conn = get_db()
    cols = conn.execute("SELECT * FROM custom_columns ORDER BY created_at").fetchall()
    conn.close()
    return {"columns": [{"id": c["id"], "name": c["name"]} for c in cols]}


@app.post("/api/columns")
def add_column(data: ColumnCreate):
    conn = get_db()
    try:
        conn.execute("INSERT INTO custom_columns (name) VALUES (?)", (data.name.strip(),))
        conn.commit()
    except Exception:
        conn.close()
        raise HTTPException(400, "Column already exists")
    conn.close()
    return {"ok": True}


@app.delete("/api/columns/{col_id}")
def delete_column(col_id: int):
    conn = get_db()
    conn.execute("DELETE FROM custom_columns WHERE id = ?", (col_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── Refresh Views (background with progress) ───────────────

DELAY_IG = 0.5       # seconds — between concurrent IG batches
DELAY_FB = 2         # seconds — Facebook/Playwright needs sequential pacing
YT_WORKERS = 3       # concurrent yt-dlp fetches
IG_WORKERS = 3       # concurrent IG fetches (v1 API is fast)

_refresh_state = {
    "running": False,
    "total": 0,
    "completed": 0,
    "errors": 0,
    "current_url": "",
    "error_details": [],
}
_refresh_lock = threading.Lock()


def _process_single_reel(reel: dict) -> None:
    """Fetch data for a single reel and persist to DB. Thread-safe."""
    reel_id, url, existing_posted = reel["id"], reel["url"], reel["posted_date"]

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

    conn = get_db()
    conn.execute(
        "INSERT INTO snapshots (reel_id, views, likes, comments) VALUES (?, ?, ?, ?)",
        (reel_id, data.get("views"), data.get("likes"), data.get("comments")),
    )

    updates, params = [], []
    if not existing_posted and data.get("posted_date"):
        updates.append("posted_date = ?")
        params.append(data["posted_date"])
    if data.get("title"):
        row = conn.execute("SELECT title FROM reels WHERE id = ?", (reel_id,)).fetchone()
        if not row["title"]:
            updates.append("title = ?")
            params.append(data["title"])
    if data.get("account"):
        row2 = conn.execute("SELECT account FROM reels WHERE id = ?", (reel_id,)).fetchone()
        if not row2["account"]:
            updates.append("account = ?")
            params.append(data["account"])
    if updates:
        params.append(reel_id)
        conn.execute(f"UPDATE reels SET {', '.join(updates)} WHERE id = ?", params)

    # Auto-populate monthly_views for current month as delta (skip if manual entry exists)
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    total_views = data.get("views")
    if total_views is not None:
        # ── Month-end backfill: if previous month has no entry, create one from last snapshot ──
        prev_month_dt = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
        prev_month = prev_month_dt.strftime("%Y-%m")
        prev_entry = conn.execute(
            "SELECT id FROM monthly_views WHERE reel_id = ? AND month = ?",
            (reel_id, prev_month),
        ).fetchone()
        if not prev_entry:
            # Find the last snapshot taken during the previous month
            prev_month_end = prev_month_dt.strftime("%Y-%m-%d 23:59:59")
            prev_month_start = prev_month + "-01 00:00:00"
            last_prev_snap = conn.execute(
                "SELECT views FROM snapshots WHERE reel_id = ? "
                "AND fetched_at >= ? AND fetched_at <= ? "
                "ORDER BY fetched_at DESC LIMIT 1",
                (reel_id, prev_month_start, prev_month_end),
            ).fetchone()
            if last_prev_snap and last_prev_snap["views"] is not None:
                # Sum of all months before the previous month
                older_sum_row = conn.execute(
                    "SELECT COALESCE(SUM(cumulative_views), 0) AS s FROM monthly_views "
                    "WHERE reel_id = ? AND month < ?",
                    (reel_id, prev_month),
                ).fetchone()
                older_sum = older_sum_row["s"]
                prev_month_views = max(last_prev_snap["views"] - older_sum, 0)
                if prev_month_views > 0:
                    conn.execute(
                        "INSERT INTO monthly_views (reel_id, month, cumulative_views, is_manual) "
                        "VALUES (?, ?, ?, 0) ON CONFLICT(reel_id, month) DO NOTHING",
                        (reel_id, prev_month, prev_month_views),
                    )
                    logger.debug("Backfilled %s views for reel %d month %s", prev_month_views, reel_id, prev_month)

        # ── Current month calculation ──
        existing = conn.execute(
            "SELECT is_manual FROM monthly_views WHERE reel_id = ? AND month = ?",
            (reel_id, current_month),
        ).fetchone()
        if not existing or not existing["is_manual"]:
            # Sum of all previous months' deltas (stored in cumulative_views column)
            prev_sum_row = conn.execute(
                "SELECT COALESCE(SUM(cumulative_views), 0) AS s FROM monthly_views "
                "WHERE reel_id = ? AND month < ?",
                (reel_id, current_month),
            ).fetchone()
            previous_months_sum = prev_sum_row["s"]
            current_month_views = max(total_views - previous_months_sum, 0)
            conn.execute(
                "INSERT INTO monthly_views (reel_id, month, cumulative_views, is_manual) "
                "VALUES (?, ?, ?, 0) "
                "ON CONFLICT(reel_id, month) DO UPDATE SET cumulative_views=excluded.cumulative_views, "
                "updated_at=datetime('now') WHERE is_manual = 0",
                (reel_id, current_month, current_month_views),
            )

    conn.commit()
    conn.close()

    with _refresh_lock:
        _refresh_state["completed"] += 1


def _process_ig(reels: list) -> None:
    """Process Instagram reels with concurrent workers."""
    with ThreadPoolExecutor(max_workers=IG_WORKERS) as pool:
        futures = {pool.submit(_process_single_reel, reel): reel for reel in reels}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.exception("IG worker error for %s", futures[future]["url"])
            time.sleep(DELAY_IG)


def _process_youtube(reels: list) -> None:
    """Process YouTube reels with up to 3 concurrent workers."""
    with ThreadPoolExecutor(max_workers=YT_WORKERS) as pool:
        futures = {pool.submit(_process_single_reel, reel): reel for reel in reels}
        for future in as_completed(futures):
            # Exceptions inside _process_single_reel are already handled,
            # but catch anything unexpected so the thread doesn't die.
            try:
                future.result()
            except Exception:
                logger.exception("YouTube worker unexpected error for %s", futures[future]["url"])


def _process_fb(reels: list) -> None:
    """Process Facebook reels sequentially with 2s delay (Playwright)."""
    for i, reel in enumerate(reels):
        _process_single_reel(reel)
        if i < len(reels) - 1:
            time.sleep(DELAY_FB)


def _run_platform_threads(by_platform: dict[str, list]):
    """Launch one thread per platform and wait for all to finish."""
    platform_handlers = {
        "instagram": _process_ig,
        "youtube": _process_youtube,
        "facebook": _process_fb,
    }
    threads: list[threading.Thread] = []
    for platform, reels in by_platform.items():
        handler = platform_handlers.get(platform, _process_ig)
        t = threading.Thread(target=handler, args=(reels,), daemon=True)
        t.name = f"refresh-{platform}"
        threads.append(t)
        t.start()
        logger.info("Refresh: started %s thread for %d reels", platform, len(reels))
    for t in threads:
        t.join()


def _refresh_worker(reel_rows: list):
    """Runs in background thread. Splits reels by platform and processes in parallel.
    After completion, detects IG cookie expiry and auto-heals."""
    from scraper import ig_auto_refresh_cookies

    # Group reels by platform
    by_platform: dict[str, list] = defaultdict(list)
    for reel in reel_rows:
        by_platform[reel["platform"]].append(reel)

    _run_platform_threads(by_platform)

    # Check for IG cookie expiry pattern: many IG errors with "Login may be required"
    with _refresh_lock:
        ig_errors = [e for e in _refresh_state["error_details"]
                     if "instagram.com" in e.get("url", "") and "Login" in e.get("error", "")]

    if len(ig_errors) >= 3 and "instagram" in by_platform:
        logger.info("Detected %d IG auth errors — attempting auto cookie refresh", len(ig_errors))
        result = ig_auto_refresh_cookies()

        if result.get("success"):
            logger.info("IG cookies refreshed — re-running %d failed IG reels", len(ig_errors))
            # Re-run only the failed IG reels
            failed_urls = {e["url"] for e in ig_errors}
            retry_reels = [r for r in by_platform["instagram"] if r["url"] in failed_urls]

            with _refresh_lock:
                # Remove old IG errors, update totals for retry
                _refresh_state["error_details"] = [
                    e for e in _refresh_state["error_details"] if e not in ig_errors]
                _refresh_state["errors"] -= len(ig_errors)
                _refresh_state["completed"] -= len(ig_errors)
                _refresh_state["total"] = _refresh_state["total"]  # keep same total
                _refresh_state["cookie_auto_refreshed"] = True

            _process_ig(retry_reels)

            with _refresh_lock:
                # Count how many still failed after retry
                new_ig_errors = [e for e in _refresh_state["error_details"]
                                 if "instagram.com" in e.get("url", "")]
                _refresh_state["cookie_retry_recovered"] = len(ig_errors) - len(new_ig_errors)
        else:
            logger.warning("IG auto cookie refresh failed: %s", result.get("error"))
            with _refresh_lock:
                _refresh_state["cookie_auto_refreshed"] = False
                _refresh_state["cookie_refresh_error"] = result.get("error", "Unknown")

    with _refresh_lock:
        _refresh_state["running"] = False
        _refresh_state["current_url"] = ""


@app.post("/api/refresh")
def refresh_views():
    with _refresh_lock:
        if _refresh_state["running"]:
            raise HTTPException(409, "Refresh already in progress")

    conn = get_db()
    reels = conn.execute("SELECT id, url, posted_date, platform FROM reels").fetchall()
    conn.close()

    if not reels:
        return {"total": 0, "message": "No reels to refresh"}

    with _refresh_lock:
        _refresh_state["running"] = True
        _refresh_state["total"] = len(reels)
        _refresh_state["completed"] = 0
        _refresh_state["errors"] = 0
        _refresh_state["error_details"] = []
        _refresh_state["current_url"] = ""
        _refresh_state["cookie_auto_refreshed"] = None
        _refresh_state["cookie_retry_recovered"] = 0
        _refresh_state["cookie_refresh_error"] = ""

    thread = threading.Thread(target=_refresh_worker, args=(list(reels),), daemon=True)
    thread.start()

    # Estimate: platforms run in parallel, so total time ~ slowest platform
    by_plat: dict[str, int] = defaultdict(int)
    for r in reels:
        by_plat[r["platform"]] += 1
    avg_fetch = 3  # seconds per fetch on average
    ig_est = by_plat.get("instagram", 0) * (DELAY_IG + avg_fetch)
    yt_est = (by_plat.get("youtube", 0) / YT_WORKERS) * avg_fetch  # concurrent
    fb_est = by_plat.get("facebook", 0) * (DELAY_FB + avg_fetch)
    est_seconds = int(max(ig_est, yt_est, fb_est, 0))

    return {"started": True, "total": len(reels), "est_seconds": est_seconds}


@app.get("/api/refresh/progress")
def refresh_progress():
    with _refresh_lock:
        return dict(_refresh_state)


@app.post("/api/refresh/reset")
def refresh_reset():
    """Force-reset a stuck refresh state."""
    with _refresh_lock:
        _refresh_state["running"] = False
        _refresh_state["current_url"] = ""
    return {"ok": True}


# ── Daily Cron Refresh (8:00 AM IST / 2:30 AM UTC) ──────────

CRON_HOUR_UTC = 2
CRON_MINUTE_UTC = 30

_cron_state = {
    "enabled": True,
    "last_run": None,   # ISO string or None
    "next_run": None,   # ISO string
}
_cron_lock = threading.Lock()


def _seconds_until_next_cron() -> float:
    """Return seconds until the next 02:30 UTC."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=CRON_HOUR_UTC, minute=CRON_MINUTE_UTC, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _next_cron_iso() -> str:
    """Return the next 02:30 UTC as a human-readable string."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=CRON_HOUR_UTC, minute=CRON_MINUTE_UTC, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target.strftime("%Y-%m-%d %H:%M UTC")


def _trigger_refresh() -> bool:
    """Trigger a refresh if one is not already running. Returns True if started."""
    with _refresh_lock:
        if _refresh_state["running"]:
            logger.info("Cron: skipping — manual refresh already in progress")
            return False

    conn = get_db()
    reels = conn.execute("SELECT id, url, posted_date, platform FROM reels").fetchall()
    conn.close()

    if not reels:
        logger.info("Cron: no reels to refresh")
        return False

    with _refresh_lock:
        _refresh_state["running"] = True
        _refresh_state["total"] = len(reels)
        _refresh_state["completed"] = 0
        _refresh_state["errors"] = 0
        _refresh_state["error_details"] = []
        _refresh_state["current_url"] = ""
        _refresh_state["cookie_auto_refreshed"] = None
        _refresh_state["cookie_retry_recovered"] = 0
        _refresh_state["cookie_refresh_error"] = ""

    thread = threading.Thread(target=_refresh_worker, args=(list(reels),), daemon=True)
    thread.start()
    return True


def _cron_loop():
    """Background daemon: sleep until 02:30 UTC, trigger refresh, repeat."""
    with _cron_lock:
        _cron_state["next_run"] = _next_cron_iso()

    logger.info("Cron: daily refresh scheduled — next run at %s", _cron_state["next_run"])

    while True:
        wait = _seconds_until_next_cron()
        logger.info("Cron: sleeping %.0f seconds until next run", wait)
        time.sleep(wait)

        logger.info("Cron: 8:00 AM IST — starting daily refresh")
        started = _trigger_refresh()

        with _cron_lock:
            _cron_state["last_run"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            _cron_state["next_run"] = _next_cron_iso()

        if started:
            logger.info("Cron: refresh started successfully")
        else:
            logger.info("Cron: refresh was not started (already running or no reels)")

        # Sleep a bit to avoid double-trigger if the loop resumes within the same minute
        time.sleep(61)


@app.get("/api/cron/status")
def cron_status():
    with _cron_lock:
        return dict(_cron_state)


# ── Snapshots ───────────────────────────────────────────────


@app.get("/api/reels/{reel_id}/snapshots")
def reel_snapshots(reel_id: int):
    conn = get_db()
    rows = conn.execute(
        "SELECT views, likes, comments, fetched_at FROM snapshots "
        "WHERE reel_id = ? ORDER BY fetched_at",
        (reel_id,),
    ).fetchall()
    conn.close()
    return {"snapshots": [dict(r) for r in rows]}


@app.post("/api/admin/backfill-march")
def backfill_march():
    """One-time migration: for reels with no March 2026 entry but April entry,
    move April views to March (since these reels were tracked in March but first
    snapshot was taken after month rollover)."""
    conn = get_db()
    reels = conn.execute("SELECT id, posted_date FROM reels").fetchall()
    moved = 0
    for r in reels:
        # Check if has April entry but no March
        apr = conn.execute(
            "SELECT cumulative_views, is_manual FROM monthly_views WHERE reel_id = ? AND month = '2026-04'",
            (r["id"],),
        ).fetchone()
        mar = conn.execute(
            "SELECT id FROM monthly_views WHERE reel_id = ? AND month = '2026-03'",
            (r["id"],),
        ).fetchone()
        if apr and not mar and not apr["is_manual"]:
            # Move April views to March, reset April
            conn.execute(
                "INSERT INTO monthly_views (reel_id, month, cumulative_views, is_manual) VALUES (?, '2026-03', ?, 0)",
                (r["id"], apr["cumulative_views"]),
            )
            conn.execute(
                "UPDATE monthly_views SET cumulative_views = 0, updated_at = datetime('now') "
                "WHERE reel_id = ? AND month = '2026-04' AND is_manual = 0",
                (r["id"],),
            )
            moved += 1
    conn.commit()
    conn.close()
    return {"moved": moved, "message": f"Moved {moved} reels' views from April to March"}


# ── Analytics ───────────────────────────────────────────────


def _month_diff(ym1: str, ym2: str) -> int:
    y1, m1 = int(ym1[:4]), int(ym1[5:7])
    y2, m2 = int(ym2[:4]), int(ym2[5:7])
    return (y1 - y2) * 12 + (m1 - m2)


def _monthly_gains_for_reel(conn, reel_id: int, posted_date: str | None = None) -> dict[str, int]:
    rows = conn.execute(
        "SELECT views, fetched_at FROM snapshots WHERE reel_id = ? AND views IS NOT NULL ORDER BY fetched_at",
        (reel_id,),
    ).fetchall()
    if not rows:
        return {}
    month_last: dict[str, int] = {}
    for r in rows:
        month_last[r["fetched_at"][:7]] = r["views"]
    months = sorted(month_last.keys())
    gains = {}
    for i, m in enumerate(months):
        if i == 0:
            # Attribute first snapshot's views to the reel's posted month,
            # not the snapshot month. E.g. reel posted Feb, first snapshot
            # in March → views go under Feb.
            target_month = posted_date[:7] if posted_date else m
            gains[target_month] = gains.get(target_month, 0) + month_last[m]
        else:
            gains[m] = gains.get(m, 0) + month_last[m] - month_last[months[i - 1]]
    return gains


@app.get("/api/analytics/monthly")
def monthly_analytics():
    conn = get_db()
    reels = conn.execute("SELECT id, posted_date FROM reels").fetchall()
    totals: dict[str, int] = {}
    for reel in reels:
        for month, gain in _monthly_gains_for_reel(conn, reel["id"], reel["posted_date"]).items():
            totals[month] = totals.get(month, 0) + max(gain, 0)
    conn.close()
    return {"months": [{"month": m, "views": v} for m, v in sorted(totals.items(), reverse=True)]}


@app.get("/api/analytics/cohort-summary")
def cohort_summary():
    conn = get_db()
    reels = conn.execute("SELECT id, posted_date, account FROM reels").fetchall()
    # month_cohorts: { calendar_month: { "M0": total, "M1": total, ... } }
    month_cohorts: dict[str, dict[str, int]] = {}
    # by_account: { calendar_month: { account_name: total_views } }
    month_accounts: dict[str, dict[str, int]] = {}

    for reel in reels:
        posted = reel["posted_date"]
        if not posted or len(posted) < 7:
            continue
        posted_ym = posted[:7]
        account = reel["account"] or "Unknown"

        # Try monthly_views first (values are already deltas), fall back to snapshots
        mv_rows = conn.execute(
            "SELECT month, cumulative_views FROM monthly_views "
            "WHERE reel_id = ? ORDER BY month",
            (reel["id"],),
        ).fetchall()

        if mv_rows:
            for mv in mv_rows:
                delta = mv["cumulative_views"] or 0
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
            # Fallback: compute from snapshots
            gains = _monthly_gains_for_reel(conn, reel["id"], posted)
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

    conn.close()
    summary = []
    for m in sorted(month_cohorts.keys(), reverse=True):
        cohorts = month_cohorts[m]
        total = sum(cohorts.values())
        by_account = month_accounts.get(m, {})
        summary.append({"month": m, "cohorts": cohorts, "total": total, "by_account": by_account})
    return {"summary": summary}


@app.get("/api/analytics/distribution")
def distribution_analytics():
    conn = get_db()
    reels = conn.execute("SELECT id, posted_date FROM reels").fetchall()
    age_views: dict[int, int] = {}
    age_reels: dict[int, set] = {}
    for reel in reels:
        posted = reel["posted_date"]
        if not posted:
            continue
        posted_ym = posted[:7]
        for month, gain in _monthly_gains_for_reel(conn, reel["id"], posted).items():
            age = _month_diff(month, posted_ym)
            if age < 0:
                continue
            age_views[age] = age_views.get(age, 0) + max(gain, 0)
            age_reels.setdefault(age, set()).add(reel["id"])
    conn.close()
    result = []
    for age in sorted(age_views.keys()):
        label = f"M{age}" + (" (posting month)" if age == 0 else "")
        result.append({"age": age, "label": label, "views": age_views[age], "reel_count": len(age_reels.get(age, set()))})
    return {"distribution": result}


@app.get("/api/analytics/pivot")
def pivot_analytics(group_by: str = "account", ids: str | None = None):
    """Pivot table. account/platform: rows=labels, cols=months. month: rows=months, cols=M0/M1/M2.
    Optional ids param: comma-separated reel IDs to filter on."""
    if group_by not in ("account", "platform", "month"):
        raise HTTPException(400, "group_by must be 'account', 'platform', or 'month'")

    id_filter = None
    if ids:
        id_filter = [int(x) for x in ids.split(",") if x.strip().isdigit()]

    conn = get_db()
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")

    if group_by == "month":
        # Cohort view: rows = calendar months, columns = M0, M1, M2...
        # M0 = views from reels posted that month, M1 = views from reels posted previous month, etc.
        if id_filter:
            placeholders = ",".join("?" * len(id_filter))
            reels = conn.execute(f"SELECT id, posted_date FROM reels WHERE posted_date IS NOT NULL AND id IN ({placeholders})", id_filter).fetchall()
        else:
            reels = conn.execute("SELECT id, posted_date FROM reels WHERE posted_date IS NOT NULL").fetchall()
        # { calendar_month: { age: total_views } }
        month_cohorts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

        for reel in reels:
            posted_ym = reel["posted_date"][:7] if len(reel["posted_date"]) >= 7 else None
            if not posted_ym:
                continue
            mv_rows = conn.execute(
                "SELECT month, cumulative_views FROM monthly_views WHERE reel_id = ? ORDER BY month",
                (reel["id"],),
            ).fetchall()
            mv_sum = 0
            for mv in mv_rows:
                views = mv["cumulative_views"] or 0
                if views > 0:
                    age = _month_diff(mv["month"], posted_ym)
                    if age >= 0:
                        month_cohorts[mv["month"]][age] += views
                    mv_sum += views
            # Current month auto
            latest = conn.execute(
                "SELECT views FROM snapshots WHERE reel_id = ? ORDER BY fetched_at DESC LIMIT 1",
                (reel["id"],),
            ).fetchone()
            total_views = latest["views"] if latest and latest["views"] is not None else 0
            auto_val = max(total_views - mv_sum, 0)
            if auto_val > 0:
                age = _month_diff(current_month, posted_ym)
                if age >= 0:
                    month_cohorts[current_month][age] += auto_val

        conn.close()

        # Find max age
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

        return {
            "months": cols,
            "rows": rows,
            "totals": {"months": dict(column_totals), "total": grand_total},
        }

    # account / platform grouping
    if id_filter:
        placeholders = ",".join("?" * len(id_filter))
        reels = conn.execute(f"SELECT id, account, platform FROM reels WHERE id IN ({placeholders})", id_filter).fetchall()
    else:
        reels = conn.execute("SELECT id, account, platform FROM reels").fetchall()
    row_data: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for reel in reels:
        label = (reel["account"] or "Unknown") if group_by == "account" else (reel["platform"] or "Unknown")
        mv_rows = conn.execute(
            "SELECT month, cumulative_views FROM monthly_views WHERE reel_id = ?",
            (reel["id"],),
        ).fetchall()
        mv_sum = 0
        for mv in mv_rows:
            views = mv["cumulative_views"] or 0
            if views > 0:
                row_data[label][mv["month"]] += views
                mv_sum += views
        latest = conn.execute(
            "SELECT views FROM snapshots WHERE reel_id = ? ORDER BY fetched_at DESC LIMIT 1",
            (reel["id"],),
        ).fetchone()
        total_views = latest["views"] if latest and latest["views"] is not None else 0
        auto_val = max(total_views - mv_sum, 0)
        if auto_val > 0:
            row_data[label][current_month] += auto_val

    conn.close()

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


# ── Export to Google Sheet ──────────────────────────────────


@app.post("/api/export-sheet")
def export_to_sheet(data: SheetExport):
    """Push all reels data to a Google Sheet. Requires GOOGLE_SERVICE_ACCOUNT_B64 env var."""
    sa_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_B64")
    if not sa_b64:
        raise HTTPException(400, "Google service account not configured. Set GOOGLE_SERVICE_ACCOUNT_B64 env var.")

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        raise HTTPException(500, "gspread not installed")

    try:
        sa_json = json.loads(base64.b64decode(sa_b64))
        creds = Credentials.from_service_account_info(sa_json, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
        ])
        gc = gspread.authorize(creds)
    except Exception as e:
        raise HTTPException(500, f"Auth failed: {e}")

    # Extract sheet ID from URL
    import re
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", data.sheet_url)
    if not m:
        raise HTTPException(400, "Invalid Google Sheets URL")
    sheet_id = m.group(1)

    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        raise HTTPException(400, f"Cannot access sheet. Share it with the service account email. Error: {e}")

    # Get all reels data
    conn = get_db()
    reels = conn.execute("SELECT * FROM reels ORDER BY created_at DESC").fetchall()

    # Collect all months across all reels
    all_months: set[str] = set()
    reel_data = []
    for r in reels:
        latest = conn.execute(
            "SELECT views, likes, comments FROM snapshots WHERE reel_id = ? ORDER BY fetched_at DESC LIMIT 1",
            (r["id"],),
        ).fetchone()
        mv_rows = conn.execute(
            "SELECT month, cumulative_views FROM monthly_views WHERE reel_id = ? ORDER BY month",
            (r["id"],),
        ).fetchall()
        mv_map = {}
        for mv in mv_rows:
            mv_map[mv["month"]] = mv["cumulative_views"] or 0
            all_months.add(mv["month"])
        reel_data.append({"reel": r, "latest": latest, "mv_map": mv_map})
    conn.close()

    sorted_months = sorted(all_months)

    # Build header
    header = ["Platform", "URL", "Account", "Title", "Posted", "Views", "Likes", "Comments"]
    header.extend([m for m in sorted_months])

    # Build rows
    rows = [header]
    for rd in reel_data:
        r = rd["reel"]
        l = rd["latest"]
        row = [
            r["platform"] or "",
            r["url"] or "",
            r["account"] or "",
            r["title"] or "",
            r["posted_date"] or "",
            l["views"] if l and l["views"] is not None else "",
            l["likes"] if l and l["likes"] is not None else "",
            l["comments"] if l and l["comments"] is not None else "",
        ]
        for m in sorted_months:
            row.append(rd["mv_map"].get(m, ""))
        rows.append(row)

    # Write to sheet
    try:
        try:
            ws = sh.worksheet("Tracker Export")
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet("Tracker Export", rows=len(rows) + 1, cols=len(header))
        ws.update(rows, value_input_option="RAW")
        return {"ok": True, "rows": len(rows) - 1, "sheet": f"Tracker Export"}
    except Exception as e:
        raise HTTPException(500, f"Failed to write: {e}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8501))
    uvicorn.run(app, host="0.0.0.0", port=port)
