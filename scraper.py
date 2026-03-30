"""Multi-platform scraper: Instagram, YouTube, Facebook.

Instagram: GraphQL API (primary) → Playwright headless (fallback)
YouTube:   yt-dlp (works accurately)
Facebook:  Playwright headless browser
"""

import asyncio
import json
import logging
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import instaloader
from instaloader.exceptions import (
    BadCredentialsException,
    TwoFactorAuthRequiredException,
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
SESSION_FILE = DATA_DIR / "ig_session"
USERNAME_FILE = DATA_DIR / "ig_username.txt"
IG_COOKIES_FILE = DATA_DIR / "ig_cookies.json"

_pending_login: dict = {"loader": None, "username": ""}

# Instagram GraphQL — doc_id rotates every few weeks, update when needed
IG_GRAPHQL_URL = "https://www.instagram.com/api/graphql"
IG_DOC_ID = "10015901848480474"
IG_APP_ID = "936619743392459"


# ── Utility ────────────────────────────────────────────────


def _parse_human_count(text: str) -> int | None:
    """Parse '1M', '770K', '1.2M', '48K', '5,234' into integers."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "")
    m = re.match(r"^([\d.]+)\s*([KkMmBb])?$", text)
    if not m:
        return None
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    num *= multipliers.get(suffix, 1)
    return int(num)


def _extract_count_from_text(text: str, patterns: list[str]) -> int | None:
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return _parse_human_count(m.group(1))
    return None


def _parse_hindi_count(text: str) -> int | None:
    """Parse Hindi number formats: '4.1 लाख', '3.5 हज़ार', '2 करोड़'."""
    hindi_multipliers = [
        (r"([\d.]+)\s*(?:लाख|lakh)", 100_000),
        (r"([\d.]+)\s*(?:हज़ार|हजार|hazar|thousand)", 1_000),
        (r"([\d.]+)\s*(?:करोड़|crore)", 10_000_000),
    ]
    for pattern, mult in hindi_multipliers:
        m = re.search(pattern, text)
        if m:
            return int(float(m.group(1)) * mult)
    return None


def _parse_fb_og_views(og_title: str) -> int | None:
    """Extract view count from Facebook og:title. Handles English and Hindi."""
    if not og_title:
        return None
    # English: "48K views", "1.2M views"
    m = re.search(r"([\d,.]+)\s*([KkMm])?\s*(?:views|Views)", og_title)
    if m:
        return _parse_human_count(m.group(1) + (m.group(2) or ""))
    # Hindi: "4.1 लाख व्यूज़"
    hindi = _parse_hindi_count(og_title.split("·")[0] if "·" in og_title else og_title)
    if hindi:
        return hindi
    return None


# ── Platform Detection ─────────────────────────────────────


def detect_platform(url: str) -> str:
    u = url.lower()
    if "instagram.com" in u:
        return "instagram"
    if "youtube.com" in u or "youtu.be" in u or "shorts" in u:
        return "youtube"
    if "facebook.com" in u or "fb.watch" in u or "fb.com" in u:
        return "facebook"
    return "unknown"


# ── Instagram Auth ─────────────────────────────────────────


def _get_ig_loader() -> instaloader.Instaloader:
    L = instaloader.Instaloader()
    username = USERNAME_FILE.read_text().strip() if USERNAME_FILE.exists() else None
    if username and SESSION_FILE.exists():
        try:
            L.load_session_from_file(username, str(SESSION_FILE))
        except Exception:
            pass
    return L


def _export_cookies_to_json(L: instaloader.Instaloader):
    """Export instaloader session cookies to Playwright-compatible JSON."""
    try:
        session = L.context._session
        cookies = []
        for cookie in session.cookies:
            domain = cookie.domain or ".instagram.com"
            cookies.append({
                "name": cookie.name,
                "value": cookie.value,
                "domain": domain,
                "path": cookie.path or "/",
                "secure": True,
                "httpOnly": bool(cookie._rest.get("HttpOnly", False)),
                "sameSite": "None",
            })
        DATA_DIR.mkdir(exist_ok=True)
        IG_COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    except Exception as e:
        logger.warning("Cookie export failed: %s", e)


def ig_is_logged_in() -> bool:
    if IG_COOKIES_FILE.exists():
        try:
            cookies = json.loads(IG_COOKIES_FILE.read_text())
            return any(c.get("name") == "sessionid" and c.get("value") for c in cookies)
        except Exception:
            pass
    if not USERNAME_FILE.exists() or not SESSION_FILE.exists():
        return False
    try:
        return _get_ig_loader().context.is_logged_in
    except Exception:
        return False


def ig_username() -> str | None:
    return USERNAME_FILE.read_text().strip() if USERNAME_FILE.exists() else None


def ig_login(username: str, password: str) -> dict:
    global _pending_login
    L = instaloader.Instaloader()
    try:
        L.login(username, password)
        DATA_DIR.mkdir(exist_ok=True)
        L.save_session_to_file(str(SESSION_FILE))
        USERNAME_FILE.write_text(username)
        _export_cookies_to_json(L)
        _pending_login = {"loader": None, "username": ""}
        return {"success": True, "username": username}
    except TwoFactorAuthRequiredException:
        _pending_login = {"loader": L, "username": username}
        return {"needs_2fa": True}
    except BadCredentialsException:
        return {"error": "Wrong username or password"}
    except Exception as e:
        return {"error": str(e)}


def ig_2fa(code: str) -> dict:
    global _pending_login
    L = _pending_login.get("loader")
    username = _pending_login.get("username", "")
    if not L:
        return {"error": "No pending login. Try again."}
    try:
        L.two_factor_login(code)
        DATA_DIR.mkdir(exist_ok=True)
        L.save_session_to_file(str(SESSION_FILE))
        USERNAME_FILE.write_text(username)
        _export_cookies_to_json(L)
        _pending_login = {"loader": None, "username": ""}
        return {"success": True, "username": username}
    except Exception as e:
        _pending_login = {"loader": None, "username": ""}
        return {"error": str(e)}


def ig_logout():
    SESSION_FILE.unlink(missing_ok=True)
    USERNAME_FILE.unlink(missing_ok=True)
    IG_COOKIES_FILE.unlink(missing_ok=True)


# ── Instagram: GraphQL API (primary) ──────────────────────


def _extract_ig_shortcode(url: str) -> str | None:
    m = re.search(r"instagram\.com/(?:reel|reels|p)/([A-Za-z0-9_-]+)", url)
    return m.group(1) if m else None


def _get_ig_cookies_dict() -> dict[str, str]:
    if IG_COOKIES_FILE.exists():
        try:
            cookies = json.loads(IG_COOKIES_FILE.read_text())
            return {c["name"]: c["value"] for c in cookies if "instagram" in c.get("domain", "")}
        except Exception:
            pass
    return {}


def _shortcode_to_media_id(shortcode: str) -> str:
    """Convert Instagram shortcode to numeric media ID."""
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    media_id = 0
    for char in shortcode:
        media_id = media_id * 64 + alphabet.index(char)
    return str(media_id)


def _fetch_ig_v1_api(shortcode: str) -> dict | None:
    """Fetch via Instagram v1 media info API — gives exact play_count matching the UI."""
    import httpx

    cookies = _get_ig_cookies_dict()
    if not cookies.get("sessionid"):
        return None

    media_id = _shortcode_to_media_id(shortcode)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "X-IG-App-ID": IG_APP_ID,
        "X-CSRFToken": cookies.get("csrftoken", ""),
    }

    try:
        resp = httpx.get(
            f"https://www.instagram.com/api/v1/media/{media_id}/info/",
            headers=headers, cookies=cookies, timeout=15, follow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        items = data.get("items", [])
        if not items:
            return None

        m = items[0]
        likes = m.get("like_count")
        if likes is not None and likes < 0:
            likes = None
        caption_text = (m.get("caption") or {}).get("text", "")

        return {
            "views": m.get("play_count") or m.get("view_count"),
            "likes": likes,
            "comments": m.get("comment_count", 0),
            "posted_date": datetime.fromtimestamp(m["taken_at"], tz=timezone.utc).strftime("%Y-%m-%d") if m.get("taken_at") else None,
            "title": caption_text[:100],
            "account": m.get("user", {}).get("username", ""),
        }
    except Exception as e:
        logger.warning("IG v1 API failed: %s", e)
        return None


# ── Instagram: Playwright fallback ────────────────────────


async def _fetch_ig_playwright(url: str) -> dict:
    """Intercept API responses from the page load to get exact data."""
    from playwright.async_api import async_playwright

    captured = {}

    async def on_response(response):
        try:
            if response.status != 200:
                return
            if not any(p in response.url for p in ["/api/graphql", "/api/v1/media/", "graphql/query"]):
                return
            body = await response.json()
            media = None
            if isinstance(body, dict):
                media = body.get("data", {}).get("xdt_shortcode_media")
                if not media:
                    items = body.get("items", [])
                    if items:
                        media = items[0]
            if media and not captured:
                views = media.get("video_play_count") or media.get("play_count") or media.get("video_view_count")
                likes = media.get("like_count") or media.get("edge_media_preview_like", {}).get("count")
                if likes is not None and likes < 0:
                    likes = None
                comments = media.get("comment_count") or media.get("edge_media_to_comment", {}).get("count", 0)
                caption_edges = media.get("edge_media_to_caption", {}).get("edges", [])
                caption = (caption_edges[0]["node"]["text"][:100] if caption_edges
                           else (media.get("caption") or {}).get("text", "")[:100])
                owner = media.get("owner", {}).get("username", "") or media.get("user", {}).get("username", "")
                ts = media.get("taken_at_timestamp") or media.get("taken_at")
                posted = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d") if ts else None
                captured.update({"views": views, "likes": likes, "comments": comments,
                                 "posted_date": posted, "title": caption, "account": owner})
        except Exception:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
        )
        if IG_COOKIES_FILE.exists():
            try:
                await context.add_cookies(json.loads(IG_COOKIES_FILE.read_text()))
            except Exception:
                pass

        page = await context.new_page()
        page.on("response", on_response)
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
        except Exception:
            pass

        if captured and captured.get("views") is not None:
            await browser.close()
            return captured

        # Text fallback
        try:
            text = await page.inner_text("body")
            views = _extract_count_from_text(text, [r"([\d,.]+[KkMmBb]?)\s*(?:views|plays)"])
            comments = _extract_count_from_text(text, [r"([\d,.]+[KkMmBb]?)\s*comments?"])
            await browser.close()
            if views:
                return {"views": views, "likes": None, "comments": comments or 0,
                        "posted_date": None, "title": "", "account": ""}
        except Exception:
            await browser.close()

        return {"error": "Could not extract view count. Login may be required."}


def fetch_instagram(url: str) -> dict:
    shortcode = _extract_ig_shortcode(url)
    if not shortcode:
        return {"error": "Invalid Instagram URL"}

    # Strategy 1: v1 media info API (fast, exact play_count)
    result = _fetch_ig_v1_api(shortcode)
    if result and result.get("views") is not None:
        return result

    # Strategy 2: Playwright headless browser fallback
    try:
        return asyncio.run(_fetch_ig_playwright(url))
    except Exception as e:
        return {"error": f"Instagram fetch failed: {e}"}


# ── YouTube (yt-dlp — works accurately) ───────────────────


def fetch_youtube(url: str) -> dict:
    try:
        result = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-download", url],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            lines = result.stderr.strip().split("\n")
            return {"error": lines[-1] if lines else "Unknown error"}
        meta = json.loads(result.stdout)
        upload = meta.get("upload_date")
        posted = f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" if upload else None
        return {
            "views": meta.get("view_count"), "likes": meta.get("like_count"),
            "comments": meta.get("comment_count"), "posted_date": posted,
            "title": (meta.get("title") or "")[:100],
            "account": meta.get("uploader") or meta.get("channel") or "",
        }
    except subprocess.TimeoutExpired:
        return {"error": "Timeout"}
    except Exception as e:
        return {"error": str(e)}


# ── Facebook: Playwright ──────────────────────────────────


async def _fetch_fb_playwright(url: str) -> dict:
    """Open FB reel, extract view count from og:title meta tag."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await (await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )).new_page()

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(2000)
        except Exception:
            pass

        result = {"views": None, "likes": None, "comments": 0,
                  "posted_date": None, "title": "", "account": ""}

        try:
            og = await page.get_attribute('meta[property="og:title"]', "content")
            if og:
                result["views"] = _parse_fb_og_views(og)
                # Clean title: strip view/reaction prefix (English + Hindi)
                clean = re.sub(r"^[\d.,]+\s*[KkMmBb]?\s*(?:views|Views)\s*·\s*[\d.,]+\s*[KkMmBb]?\s*(?:reactions?|Reactions?)\s*\|\s*", "", og)
                clean = re.sub(r"^[\d.]+\s*(?:लाख|हज़ार|करोड़)\s*(?:व्यूज़?)\s*·\s*[\d.]+\s*(?:लाख|हज़ार|करोड़)?\s*(?:रिएक्शन)\s*\|\s*", "", clean)
                parts = clean.rsplit(" | ", 1)
                if len(parts) == 2:
                    result["account"] = parts[1].strip()
                    clean = parts[0]
                result["title"] = clean.strip()[:100]

            if not result["account"]:
                for selector in ['a[role="link"] strong', 'h2 a span']:
                    el = await page.query_selector(selector)
                    if el:
                        result["account"] = (await el.inner_text()).strip()
                        break
        except Exception:
            pass

        await browser.close()
        if result["views"] is not None:
            return result
        return {"error": "Could not extract view count from Facebook page."}


def _fetch_fb_ytdlp(url: str) -> dict:
    """Fallback: yt-dlp for Facebook."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-download", url],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return {"error": result.stderr.strip().split("\n")[-1]}
        meta = json.loads(result.stdout)
        upload = meta.get("upload_date")
        posted = f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" if upload else None
        raw_title = meta.get("title") or ""
        uploader = meta.get("uploader") or ""
        views = _parse_fb_og_views(raw_title) or meta.get("view_count")
        clean = re.sub(r"^[\d.]+[KkMm]?\s*views\s*·\s*[\d.]+[KkMm]?\s*reactions?\s*\|\s*", "", raw_title)
        if uploader and clean.endswith(f" | {uploader}"):
            clean = clean[:-(len(uploader) + 3)]
        return {
            "views": views, "likes": meta.get("like_count"), "comments": meta.get("comment_count"),
            "posted_date": posted, "title": clean.strip()[:100], "account": uploader,
        }
    except Exception as e:
        return {"error": str(e)}


def fetch_facebook(url: str) -> dict:
    try:
        result = asyncio.run(_fetch_fb_playwright(url))
        if "error" not in result:
            return result
    except Exception:
        pass
    return _fetch_fb_ytdlp(url)


# ── Router ─────────────────────────────────────────────────


def fetch_reel_data(url: str) -> dict:
    platform = detect_platform(url)
    if platform == "instagram":
        return fetch_instagram(url)
    if platform == "youtube":
        return fetch_youtube(url)
    if platform == "facebook":
        return fetch_facebook(url)
    return {"error": "Unsupported platform. Use Instagram, YouTube, or Facebook URLs."}
