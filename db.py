"""Supabase Postgres database for Social Branding Tracker (multi-tenant SaaS)."""

import os
import json
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://qutgezcgynqxqdtcgmfz.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

_client: Client | None = None


def get_supabase() -> Client:
    """Get Supabase client (service role — bypasses RLS for backend operations)."""
    global _client
    if _client is None:
        if not SUPABASE_SERVICE_KEY:
            raise RuntimeError("SUPABASE_SERVICE_KEY env var required")
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return _client


def init_db():
    """Verify connection to Supabase. Tables are created via SQL migration."""
    sb = get_supabase()
    # Quick connectivity check
    try:
        sb.table("tracker_profiles").select("id").limit(1).execute()
    except Exception as e:
        raise RuntimeError(f"Supabase connection failed: {e}")


# ── Profile helpers ──────────────────────────────────────────


def get_user_tier(user_id: str) -> str:
    """Get user's tier (free/paid). Defaults to free."""
    sb = get_supabase()
    res = sb.table("tracker_profiles").select("tier").eq("id", user_id).execute()
    if res.data:
        return res.data[0].get("tier", "free")
    return "free"


# ── Reels CRUD ───────────────────────────────────────────────


def list_reels(user_id: str, month: str | None = None) -> list[dict]:
    sb = get_supabase()
    query = sb.table("tracker_reels").select("*").eq("user_id", user_id).order("created_at", desc=True)
    if month and month != "all":
        query = query.like("posted_date", f"{month}%")
    res = query.execute()
    return res.data


def get_reel(user_id: str, reel_id: int) -> dict | None:
    sb = get_supabase()
    res = sb.table("tracker_reels").select("*").eq("id", reel_id).eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


def insert_reel(user_id: str, url: str, title: str, platform: str) -> dict:
    sb = get_supabase()
    res = sb.table("tracker_reels").insert({
        "user_id": user_id,
        "url": url.strip(),
        "title": title.strip(),
        "platform": platform,
    }).execute()
    return res.data[0] if res.data else {}


def update_reel(user_id: str, reel_id: int, updates: dict) -> bool:
    sb = get_supabase()
    res = sb.table("tracker_reels").update(updates).eq("id", reel_id).eq("user_id", user_id).execute()
    return bool(res.data)


def delete_reel(user_id: str, reel_id: int) -> bool:
    sb = get_supabase()
    # Cascade deletes snapshots and monthly_views via FK
    res = sb.table("tracker_reels").delete().eq("id", reel_id).eq("user_id", user_id).execute()
    return bool(res.data)


def bulk_delete_reels(user_id: str, ids: list[int]) -> int:
    sb = get_supabase()
    deleted = 0
    for rid in ids:
        res = sb.table("tracker_reels").delete().eq("id", rid).eq("user_id", user_id).execute()
        if res.data:
            deleted += 1
    return deleted


# ── Snapshots ────────────────────────────────────────────────


def insert_snapshot(user_id: str, reel_id: int, views: int | None, likes: int | None, comments: int | None):
    sb = get_supabase()
    sb.table("tracker_snapshots").insert({
        "user_id": user_id,
        "reel_id": reel_id,
        "views": views,
        "likes": likes,
        "comments": comments,
    }).execute()


def get_latest_snapshot(reel_id: int) -> dict | None:
    sb = get_supabase()
    res = sb.table("tracker_snapshots").select("views, likes, comments, fetched_at").eq(
        "reel_id", reel_id
    ).order("fetched_at", desc=True).limit(1).execute()
    return res.data[0] if res.data else None


def get_prev_snapshot(reel_id: int) -> dict | None:
    sb = get_supabase()
    res = sb.table("tracker_snapshots").select("views").eq(
        "reel_id", reel_id
    ).order("fetched_at", desc=True).limit(2).execute()
    return res.data[1] if len(res.data) >= 2 else None


def get_snapshots_in_range(reel_id: int, start: str, end: str) -> list[dict]:
    sb = get_supabase()
    res = sb.table("tracker_snapshots").select("views").eq("reel_id", reel_id).gte(
        "fetched_at", start
    ).lte("fetched_at", end).order("fetched_at", desc=True).limit(1).execute()
    return res.data


def get_reel_snapshots(reel_id: int) -> list[dict]:
    sb = get_supabase()
    res = sb.table("tracker_snapshots").select("views, likes, comments, fetched_at").eq(
        "reel_id", reel_id
    ).order("fetched_at").execute()
    return res.data


# ── Monthly Views ────────────────────────────────────────────


def get_monthly_views(reel_id: int) -> list[dict]:
    sb = get_supabase()
    res = sb.table("tracker_monthly_views").select("month, cumulative_views, is_manual").eq(
        "reel_id", reel_id
    ).order("month").execute()
    return res.data


def get_monthly_view_entry(reel_id: int, month: str) -> dict | None:
    sb = get_supabase()
    res = sb.table("tracker_monthly_views").select("id, is_manual").eq(
        "reel_id", reel_id
    ).eq("month", month).execute()
    return res.data[0] if res.data else None


def upsert_monthly_views(user_id: str, reel_id: int, month: str, views: int, is_manual: bool = False):
    sb = get_supabase()
    sb.table("tracker_monthly_views").upsert({
        "user_id": user_id,
        "reel_id": reel_id,
        "month": month,
        "cumulative_views": views,
        "is_manual": is_manual,
        "updated_at": "now()",
    }, on_conflict="reel_id,month").execute()


def upsert_monthly_views_auto(user_id: str, reel_id: int, month: str, views: int):
    """Insert or update only if NOT manually set."""
    existing = get_monthly_view_entry(reel_id, month)
    if existing and existing.get("is_manual"):
        return  # Don't overwrite manual entries
    upsert_monthly_views(user_id, reel_id, month, views, is_manual=False)


def delete_future_auto_monthly(reel_id: int, after_month: str):
    sb = get_supabase()
    sb.table("tracker_monthly_views").delete().eq(
        "reel_id", reel_id
    ).gt("month", after_month).eq("is_manual", False).execute()


def sum_monthly_views_before(reel_id: int, before_month: str) -> int:
    sb = get_supabase()
    res = sb.table("tracker_monthly_views").select("cumulative_views").eq(
        "reel_id", reel_id
    ).lt("month", before_month).execute()
    return sum(r.get("cumulative_views", 0) or 0 for r in res.data)


# ── Custom Columns ───────────────────────────────────────────


def list_columns(user_id: str) -> list[dict]:
    sb = get_supabase()
    res = sb.table("tracker_custom_columns").select("id, name").eq(
        "user_id", user_id
    ).order("created_at").execute()
    return res.data


def insert_column(user_id: str, name: str) -> bool:
    sb = get_supabase()
    try:
        sb.table("tracker_custom_columns").insert({
            "user_id": user_id,
            "name": name.strip(),
        }).execute()
        return True
    except Exception:
        return False


def delete_column(user_id: str, col_id: int) -> bool:
    sb = get_supabase()
    res = sb.table("tracker_custom_columns").delete().eq("id", col_id).eq("user_id", user_id).execute()
    return bool(res.data)


# ── Months listing ───────────────────────────────────────────


def list_months(user_id: str) -> list[str]:
    sb = get_supabase()
    # Get distinct months from posted_date
    reels = sb.table("tracker_reels").select("posted_date").eq("user_id", user_id).not_.is_("posted_date", "null").execute()
    reel_months = set()
    for r in reels.data:
        pd = r.get("posted_date", "")
        if pd and len(pd) >= 7:
            reel_months.add(pd[:7])

    # Get distinct months from monthly_views
    mv = sb.table("tracker_monthly_views").select("month").eq("user_id", user_id).execute()
    mv_months = set(r["month"] for r in mv.data if r.get("month"))

    return sorted(reel_months | mv_months, reverse=True)


# ── Analytics helpers ────────────────────────────────────────


def get_all_reels_for_analytics(user_id: str) -> list[dict]:
    sb = get_supabase()
    res = sb.table("tracker_reels").select("id, posted_date, account, platform").eq("user_id", user_id).execute()
    return res.data


def get_snapshots_for_reel(reel_id: int) -> list[dict]:
    sb = get_supabase()
    res = sb.table("tracker_snapshots").select("views, fetched_at").eq(
        "reel_id", reel_id
    ).not_.is_("views", "null").order("fetched_at").execute()
    return res.data
