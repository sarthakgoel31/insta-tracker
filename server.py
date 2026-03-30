"""FastAPI server for Social Tracker."""

import json
import threading
import time
from datetime import datetime

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

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return FileResponse("static/index.html")


# ── Models ──────────────────────────────────────────────────


class ReelCreate(BaseModel):
    url: str
    title: str = ""


class ReelUpdate(BaseModel):
    title: str | None = None
    posted_date: str | None = None
    custom_fields: dict | None = None


class ColumnCreate(BaseModel):
    name: str


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


# ── Reels CRUD ──────────────────────────────────────────────


@app.get("/api/reels")
def list_reels():
    conn = get_db()
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

DELAY_BETWEEN_REQUESTS = 2  # seconds — avoids rate limits

_refresh_state = {
    "running": False,
    "total": 0,
    "completed": 0,
    "errors": 0,
    "current_url": "",
}
_refresh_lock = threading.Lock()


def _refresh_worker(reel_rows: list):
    """Runs in background thread. Fetches one reel at a time with delays."""
    for i, reel in enumerate(reel_rows):
        reel_id, url, existing_posted = reel["id"], reel["url"], reel["posted_date"]

        with _refresh_lock:
            _refresh_state["current_url"] = url

        data = fetch_reel_data(url)

        if "error" in data:
            with _refresh_lock:
                _refresh_state["completed"] += 1
                _refresh_state["errors"] += 1
            if i < len(reel_rows) - 1:
                time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

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

        conn.commit()
        conn.close()

        with _refresh_lock:
            _refresh_state["completed"] += 1

        if i < len(reel_rows) - 1:
            time.sleep(DELAY_BETWEEN_REQUESTS)

    with _refresh_lock:
        _refresh_state["running"] = False
        _refresh_state["current_url"] = ""


@app.post("/api/refresh")
def refresh_views():
    with _refresh_lock:
        if _refresh_state["running"]:
            raise HTTPException(409, "Refresh already in progress")

    conn = get_db()
    reels = conn.execute("SELECT id, url, posted_date FROM reels").fetchall()
    conn.close()

    if not reels:
        return {"total": 0, "message": "No reels to refresh"}

    with _refresh_lock:
        _refresh_state["running"] = True
        _refresh_state["total"] = len(reels)
        _refresh_state["completed"] = 0
        _refresh_state["errors"] = 0
        _refresh_state["current_url"] = ""

    thread = threading.Thread(target=_refresh_worker, args=(list(reels),), daemon=True)
    thread.start()

    return {"started": True, "total": len(reels), "est_seconds": len(reels) * (DELAY_BETWEEN_REQUESTS + 3)}


@app.get("/api/refresh/progress")
def refresh_progress():
    with _refresh_lock:
        return dict(_refresh_state)


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


# ── Analytics ───────────────────────────────────────────────


def _month_diff(ym1: str, ym2: str) -> int:
    y1, m1 = int(ym1[:4]), int(ym1[5:7])
    y2, m2 = int(ym2[:4]), int(ym2[5:7])
    return (y1 - y2) * 12 + (m1 - m2)


def _monthly_gains_for_reel(conn, reel_id: int) -> dict[str, int]:
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
        gains[m] = month_last[m] - (month_last[months[i - 1]] if i > 0 else 0)
    return gains


@app.get("/api/analytics/monthly")
def monthly_analytics():
    conn = get_db()
    reels = conn.execute("SELECT id FROM reels").fetchall()
    totals: dict[str, int] = {}
    for reel in reels:
        for month, gain in _monthly_gains_for_reel(conn, reel["id"]).items():
            totals[month] = totals.get(month, 0) + max(gain, 0)
    conn.close()
    return {"months": [{"month": m, "views": v} for m, v in sorted(totals.items(), reverse=True)]}


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
        for month, gain in _monthly_gains_for_reel(conn, reel["id"]).items():
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


if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8501))
    uvicorn.run(app, host="0.0.0.0", port=port)
