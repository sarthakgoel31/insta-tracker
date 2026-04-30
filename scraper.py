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
_ig_logged_out: bool = False  # Set by ig_logout() to suppress env var cookies
_ig_checkpointed: bool = False  # Set when v1 API returns checkpoint_required

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


def _restore_ig_session_from_env():
    """On startup, restore session file from IG_SESSION_B64 env var if file doesn't exist."""
    import base64
    if SESSION_FILE.exists():
        return
    env = os.environ.get("IG_SESSION_B64")
    if not env:
        return
    try:
        DATA_DIR.mkdir(exist_ok=True)
        SESSION_FILE.write_bytes(base64.b64decode(env))
        if not USERNAME_FILE.exists():
            USERNAME_FILE.write_text("testaccountforviews")
        logger.info("Restored IG session from env var")
    except Exception as e:
        logger.warning("Failed to restore IG session from env: %s", e)


_restore_ig_session_from_env()


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
    cookies = _get_ig_cookies_list()
    if cookies:
        return any(c.get("name") == "sessionid" and c.get("value") for c in cookies)
    if not USERNAME_FILE.exists() or not SESSION_FILE.exists():
        return False
    try:
        return _get_ig_loader().context.is_logged_in
    except Exception:
        return False


def ig_username() -> str | None:
    if USERNAME_FILE.exists():
        return USERNAME_FILE.read_text().strip()
    # Try to get from cookies (ds_user_id doesn't give username, but check env)
    cookies = _get_ig_cookies_dict()
    if cookies.get("sessionid"):
        return cookies.get("ds_user_id", "")
    return None


def ig_login(username: str, password: str) -> dict:
    global _pending_login, _ig_logged_out
    L = instaloader.Instaloader()
    try:
        L.login(username, password)
        DATA_DIR.mkdir(exist_ok=True)
        L.save_session_to_file(str(SESSION_FILE))
        USERNAME_FILE.write_text(username)
        _export_cookies_to_json(L)
        _pending_login = {"loader": None, "username": ""}
        _ig_logged_out = False
        return {"success": True, "username": username}
    except TwoFactorAuthRequiredException:
        _pending_login = {"loader": L, "username": username}
        return {"needs_2fa": True}
    except BadCredentialsException:
        return {"error": "Wrong username or password"}
    except Exception as e:
        return {"error": str(e)}


def ig_2fa(code: str) -> dict:
    global _pending_login, _ig_logged_out
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
        _ig_logged_out = False
        return {"success": True, "username": username}
    except Exception as e:
        _pending_login = {"loader": None, "username": ""}
        return {"error": str(e)}


def ig_logout():
    global _ig_logged_out
    SESSION_FILE.unlink(missing_ok=True)
    USERNAME_FILE.unlink(missing_ok=True)
    IG_COOKIES_FILE.unlink(missing_ok=True)
    _ig_logged_out = True


def ig_auto_refresh_cookies() -> dict:
    """Auto-login via Instagram web API to get fresh sessionid cookies.
    Uses IG_USERNAME + IG_PASSWORD env vars. Returns {success, cookies_count} or {error}."""
    global _ig_logged_out
    import httpx

    username = os.environ.get("IG_USERNAME", "")
    password = os.environ.get("IG_PASSWORD", "")
    if not username or not password:
        return {"error": "IG_USERNAME or IG_PASSWORD env var not set"}

    try:
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
        client = httpx.Client(follow_redirects=True, timeout=20)

        # Delete stale cookie file so env var is used for CSRF
        IG_COOKIES_FILE.unlink(missing_ok=True)

        # Get CSRF token — first try existing cookies (from env var now), then fetch from pages
        existing_cookies = _get_ig_cookies_dict()
        csrf = existing_cookies.get("csrftoken", "")

        if not csrf:
            for url in ["https://www.instagram.com/accounts/login/",
                         "https://www.instagram.com/",
                         "https://www.instagram.com/web/__mid/"]:
                try:
                    r = client.get(url, headers={"User-Agent": ua})
                    csrf = r.cookies.get("csrftoken", "") or client.cookies.get("csrftoken", "")
                    if csrf:
                        break
                except Exception:
                    continue
        if not csrf:
            return {"error": "Could not get CSRF token"}

        # Build login cookies from existing session (mid, ig_did help avoid challenges)
        login_cookies = {"csrftoken": csrf}
        for key in ["mid", "ig_did", "datr"]:
            if existing_cookies.get(key):
                login_cookies[key] = existing_cookies[key]

        # Login
        r2 = client.post("https://www.instagram.com/accounts/login/ajax/", headers={
            "User-Agent": ua, "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.instagram.com/accounts/login/",
            "X-IG-App-ID": IG_APP_ID, "X-CSRFToken": csrf,
        }, data={
            "enc_password": f"#PWD_INSTAGRAM_BROWSER:0:0:{password}",
            "username": username, "queryParams": "{}",
        }, cookies=login_cookies)

        data = r2.json()
        if not data.get("authenticated"):
            return {"error": f"Login failed: {data.get('message', 'not authenticated')}"}

        # Export cookies
        cookies = dict(client.cookies)
        if not cookies.get("sessionid"):
            return {"error": "Login succeeded but no sessionid in response"}

        cookie_list = [{"name": n, "value": v, "domain": ".instagram.com", "path": "/",
                        "secure": True, "httpOnly": False, "sameSite": "None"}
                       for n, v in cookies.items()]

        DATA_DIR.mkdir(exist_ok=True)
        IG_COOKIES_FILE.write_text(json.dumps(cookie_list, indent=2))
        USERNAME_FILE.write_text(username)
        _ig_logged_out = False

        _ig_checkpointed = False  # Reset checkpoint flag on successful login
        logger.info("Auto-refreshed IG cookies for %s (%d cookies)", username, len(cookie_list))
        return {"success": True, "cookies_count": len(cookie_list)}
    except Exception as e:
        logger.warning("IG auto-refresh failed: %s", e)
        return {"error": str(e)}


# ── Instagram: GraphQL API (primary) ──────────────────────


def _extract_ig_shortcode(url: str) -> str | None:
    m = re.search(r"instagram\.com/(?:reel|reels|p)/([A-Za-z0-9_-]+)", url)
    return m.group(1) if m else None


def _get_ig_cookies_list() -> list[dict]:
    """Load IG cookies from file or IG_COOKIES_B64 env var."""
    # File always takes priority (written by login, deleted by logout)
    if IG_COOKIES_FILE.exists():
        try:
            return json.loads(IG_COOKIES_FILE.read_text())
        except Exception:
            pass
    # Skip env var if user explicitly logged out this session
    if _ig_logged_out:
        return []
    env = os.environ.get("IG_COOKIES_B64")
    if env:
        import base64
        try:
            return json.loads(base64.b64decode(env))
        except Exception:
            pass
    return []


def _get_ig_cookies_dict() -> dict[str, str]:
    cookies = _get_ig_cookies_list()
    return {c["name"]: c["value"] for c in cookies if "instagram" in c.get("domain", "")}


def _shortcode_to_media_id(shortcode: str) -> str:
    """Convert Instagram shortcode to numeric media ID."""
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    media_id = 0
    for char in shortcode:
        media_id = media_id * 64 + alphabet.index(char)
    return str(media_id)


def _fetch_ig_v1_api(shortcode: str) -> dict | None:
    """Fetch via Instagram v1 media info API — tries www then mobile (i.instagram.com)."""
    import httpx

    cookies = _get_ig_cookies_dict()
    if not cookies.get("sessionid"):
        return None

    media_id = _shortcode_to_media_id(shortcode)

    global _ig_checkpointed

    # Try two endpoints: www (best play_count) then mobile (fallback, still good)
    endpoints = []
    if not _ig_checkpointed:
        endpoints.append({
            "url": f"https://www.instagram.com/api/v1/media/{media_id}/info/",
            "headers": {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
                "X-IG-App-ID": IG_APP_ID,
                "X-CSRFToken": cookies.get("csrftoken", ""),
            },
            "name": "www",
        })
    endpoints.append({
        "url": f"https://i.instagram.com/api/v1/media/{media_id}/info/",
        "headers": {
            "User-Agent": "Instagram 317.0.0.0.64 Android (31/12; 420dpi; 1080x2340; samsung; SM-G991B; o1s; exynos2100; en_US)",
            "X-IG-App-ID": "567067343352427",
        },
        "name": "mobile",
    })

    for ep in endpoints:
        try:
            resp = httpx.get(ep["url"], headers=ep["headers"], cookies=cookies,
                             timeout=10, follow_redirects=True)
            if resp.status_code != 200:
                if ep["name"] == "www" and resp.status_code == 400 and "checkpoint" in resp.text[:200].lower():
                    _ig_checkpointed = True
                    logger.warning("IG checkpoint on www — falling back to mobile API")
                continue
            data = resp.json()
            items = data.get("items", [])
            if not items:
                continue

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
            logger.warning("IG v1 API (%s) failed: %s", ep["name"], e)
            continue

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
        ig_cookies = _get_ig_cookies_list()
        if ig_cookies:
            try:
                await context.add_cookies(ig_cookies)
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


_ig_instaloader_last_error = ""

def _fetch_ig_instaloader(shortcode: str) -> dict | None:
    """Use instaloader with saved session to fetch post data. Uses GraphQL (different from v1 API)."""
    global _ig_instaloader_last_error
    try:
        L = _get_ig_loader()
        if not L.context.is_logged_in:
            _ig_instaloader_last_error = "not logged in"
            return None
        post = instaloader.Post.from_shortcode(L.context, shortcode)
        _ig_instaloader_last_error = ""
        # Prefer video_play_count (matches app) over video_view_count (lower)
        views = post._node.get("video_play_count") or post.video_view_count
        return {
            "views": views,
            "likes": post.likes,
            "comments": post.comments,
            "posted_date": post.date_utc.strftime("%Y-%m-%d") if post.date_utc else None,
            "title": (post.caption or "")[:100],
            "account": post.owner_username or "",
        }
    except Exception as e:
        _ig_instaloader_last_error = str(e)
        logger.warning("Instaloader fetch failed for %s: %s", shortcode, e)
        return None


def _fetch_ig_embed(shortcode: str) -> dict | None:
    """Fetch via Instagram embed page — no auth needed, works from any IP."""
    import httpx

    try:
        resp = httpx.get(
            f"https://www.instagram.com/reel/{shortcode}/embed/",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=15, follow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        html = resp.text

        views = None
        # Prefer video_play_count (matches app) over video_view_count (lower metric)
        for pattern in [r'"video_play_count":(\d+)', r'"play_count":(\d+)',
                        r'"video_view_count":(\d+)', r'"video_views":(\d+)']:
            for p in [pattern, pattern.replace('"', r'\\"')]:
                m = re.search(p, html)
                if m:
                    views = int(m.group(1))
                    break
            if views is not None:
                break

        if views is None:
            return None

        # Extract other metadata
        likes = None
        for p in [r'"edge_media_preview_like":\{[^}]*"count":(\d+)', r'\\"edge_media_preview_like\\":\{[^}]*\\"count\\":(\d+)',
                  r'"like_count":(\d+)', r'\\"like_count\\":(\d+)']:
            m = re.search(p, html)
            if m:
                likes = int(m.group(1))
                break

        comments = None
        for p in [r'"edge_media_to_comment":\{[^}]*"count":(\d+)', r'\\"edge_media_to_comment\\":\{[^}]*\\"count\\":(\d+)',
                  r'"comment_count":(\d+)', r'\\"comment_count\\":(\d+)']:
            m = re.search(p, html)
            if m:
                comments = int(m.group(1))
                break

        account = ""
        for p in [r'"owner":\{[^}]*"username":"([^"]+)"', r'\\"owner\\":\{[^}]*\\"username\\":\\"([^\\]+)\\"']:
            m = re.search(p, html)
            if m:
                account = m.group(1)
                break

        posted = None
        for p in [r'"taken_at_timestamp":(\d+)', r'\\"taken_at_timestamp\\":(\d+)']:
            m = re.search(p, html)
            if m:
                posted = datetime.fromtimestamp(int(m.group(1)), tz=timezone.utc).strftime("%Y-%m-%d")
                break

        caption = ""
        for p in [r'"text":"((?:[^"\\]|\\.)*)"', r'\\"text\\":\\"((?:[^\\]|\\.)*?)\\"']:
            m = re.search(p, html)
            if m:
                raw = m.group(1).replace('\\"', '"').replace("\\n", " ").replace("\\\\", "\\")
                # Clean up any JSON artifacts
                caption = re.sub(r'["\}{\[\]].*', '', raw).strip()[:100]
                break

        return {
            "views": views, "likes": likes, "comments": comments or 0,
            "posted_date": posted, "title": caption, "account": account,
        }
    except Exception as e:
        logger.warning("IG embed fetch failed for %s: %s", shortcode, e)
        return None


def fetch_instagram(url: str) -> dict:
    shortcode = _extract_ig_shortcode(url)
    if not shortcode:
        return {"error": "Invalid Instagram URL"}

    # Strategy 1: v1 media info API (play_count = what IG app shows)
    result = _fetch_ig_v1_api(shortcode)
    if result and result.get("views") is not None:
        result["_source"] = "v1_api"
        return result

    # Strategy 2: Instaloader with saved session (video_play_count = matches app)
    result = _fetch_ig_instaloader(shortcode)
    if result and result.get("views") is not None:
        result["_source"] = "instaloader"
        return result

    # Strategy 3: Embed page (no auth — video_play_count if available, else video_view_count)
    result = _fetch_ig_embed(shortcode)
    if result and result.get("views") is not None:
        result["_source"] = "embed"
        return result

    # Strategy 4: Playwright headless browser fallback
    try:
        return asyncio.run(_fetch_ig_playwright(url))
    except Exception as e:
        return {"error": f"Instagram fetch failed: {e}"}


# ── YouTube (yt-dlp — works accurately) ───────────────────


def _extract_yt_video_id(url: str) -> str | None:
    """Extract video ID from various YouTube URL formats."""
    patterns = [
        r"(?:youtube\.com/shorts/|youtube\.com/watch\?v=|youtu\.be/)([\w-]{11})",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def fetch_youtube(url: str) -> dict:
    """Fetch YouTube video data via direct HTML scraping (no yt-dlp, no cookies needed)."""
    import httpx

    video_id = _extract_yt_video_id(url)
    if not video_id:
        return {"error": "Could not extract YouTube video ID from URL"}

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = httpx.get(f"https://www.youtube.com/shorts/{video_id}",
                         headers=headers, timeout=15, follow_redirects=True)
        html = resp.text

        # Extract viewCount from embedded JSON
        view_match = re.search(r'"viewCount":"(\d+)"', html)
        views = int(view_match.group(1)) if view_match else None

        # Extract title from og:title (most reliable)
        title_match = re.search(r'<meta property="og:title" content="(.*?)"', html)
        title = title_match.group(1)[:100] if title_match else ""

        # Extract author/channel from videoDetails
        author_match = re.search(r'"videoDetails".*?"author":"(.*?)"', html)
        if not author_match:
            author_match = re.search(r'"author":"(.*?)"', html)
        account = author_match.group(1) if author_match else ""

        # Extract upload date from microformat
        date_match = re.search(r'"uploadDate":"(\d{4}-\d{2}-\d{2})', html)
        posted = date_match.group(1) if date_match else None

        # Extract likes from accessibility label
        likes = None
        likes_match = re.search(r'"accessibilityData":\{"label":"([\d,]+)\s+likes"', html)
        if likes_match:
            likes = int(likes_match.group(1).replace(",", ""))

        if views is not None:
            return {
                "views": views, "likes": likes, "comments": None,
                "posted_date": posted, "title": title, "account": account,
            }

        # Fallback: try yt-dlp
        return _fetch_yt_dlp(video_id, url)
    except Exception as e:
        # Fallback: try yt-dlp
        try:
            return _fetch_yt_dlp(video_id, url)
        except Exception:
            pass
        return {"error": f"YouTube fetch failed: {e}"}


def _fetch_yt_dlp(video_id: str, url: str) -> dict:
    """Fallback YouTube fetcher using Invidious API (no cookies needed)."""
    import httpx

    # Try multiple Invidious instances
    instances = [
        "https://vid.puffyan.us",
        "https://inv.tux.pizza",
        "https://invidious.fdn.fr",
        "https://y.com.sb",
    ]

    headers = {"User-Agent": "Mozilla/5.0"}

    for instance in instances:
        try:
            resp = httpx.get(
                f"{instance}/api/v1/videos/{video_id}?fields=viewCount,likeCount,title,author,published",
                headers=headers, timeout=10
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            published = data.get("published", 0)
            posted_date = None
            if published:
                from datetime import datetime, timezone
                posted_date = datetime.fromtimestamp(published, tz=timezone.utc).strftime("%Y-%m-%d")
            return {
                "views": data.get("viewCount"),
                "likes": data.get("likeCount"),
                "comments": None,
                "posted_date": posted_date,
                "title": (data.get("title") or "")[:100],
                "account": data.get("author") or "",
            }
        except Exception:
            continue

    return {"error": "YouTube blocked this server. Try again later."}


# ── Facebook: Playwright ──────────────────────────────────


FB_COOKIES_FILE = DATA_DIR / "fb_cookies.json"


def _get_fb_cookies() -> list[dict]:
    if FB_COOKIES_FILE.exists():
        try:
            return json.loads(FB_COOKIES_FILE.read_text())
        except Exception:
            pass
    env = os.environ.get("FB_COOKIES_B64")
    if env:
        import base64
        try:
            return json.loads(base64.b64decode(env))
        except Exception:
            pass
    return []


def _ocr_views_from_screenshot(img_bytes: bytes) -> int | None:
    """OCR a screenshot to find 'XXXK Views' pattern."""
    try:
        import pytesseract
        from PIL import Image
        import io

        img = Image.open(io.BytesIO(img_bytes))
        text = pytesseract.image_to_string(img)
        m = re.search(r"([\d,.]+[KkMm]?)\s*[Vv]iews", text)
        if m:
            return _parse_human_count(m.group(1))
    except Exception:
        pass  # tesseract not installed
    return None


async def _fetch_fb_playwright(url: str) -> dict:
    """Open FB reel on mobile with cookies, try multiple strategies for view count."""
    from playwright.async_api import async_playwright

    api_views = [None]

    async def on_response(response):
        """Intercept FB API responses for play_count/view_count."""
        try:
            if response.status != 200:
                return
            if any(p in response.url for p in ["/api/graphql", "/ajax/", "graphql"]):
                body = await response.text()
                for key in ["play_count", "video_play_count", "view_count"]:
                    m = re.search(rf'"{key}":\s*(\d+)', body)
                    if m and api_views[0] is None:
                        api_views[0] = int(m.group(1))
        except Exception:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 390, "height": 844},
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            is_mobile=True,
        )

        cookies = _get_fb_cookies()
        if cookies:
            await context.add_cookies(cookies)

        page = await context.new_page()
        page.on("response", on_response)

        try:
            await page.goto(url.replace("www.facebook.com", "m.facebook.com"),
                            wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            for dismiss_text in ["Stay on professional mode", "Not now", "Close", "OK"]:
                try:
                    btn = page.get_by_text(dismiss_text, exact=False).first
                    if await btn.is_visible(timeout=1000):
                        await btn.click()
                        await page.wait_for_timeout(1000)
                except Exception:
                    pass
            await page.wait_for_timeout(3000)
        except Exception:
            pass

        result = {"views": None, "likes": None, "comments": 0,
                  "posted_date": None, "title": "", "account": ""}

        # Strategy 1: API interception
        if api_views[0]:
            result["views"] = api_views[0]

        # Strategy 2: Screenshot + OCR
        if result["views"] is None:
            try:
                screenshot = await page.screenshot()
                result["views"] = _ocr_views_from_screenshot(screenshot)
            except Exception:
                pass

        # Get metadata from page text
        try:
            text = await page.inner_text("body")
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            # Account: line before "•" + "Follow"
            for i, line in enumerate(lines):
                if line == "•" and i + 1 < len(lines) and ("Follow" in lines[i + 1] or "फ़ॉलो" in lines[i + 1]):
                    if i > 0:
                        name = re.sub(r"[^\w\s.'-]", "", lines[i - 1]).strip()
                        if name and len(name) > 1:
                            result["account"] = name

                    # Parse date from nearby lines (FB shows dates like "Feb 27", "Mar 22" near account)
                    # Check a few lines around the account name / "•" / "Follow" area
                    date_months = {
                        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
                    }
                    date_pattern = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}"
                    search_start = max(0, i - 3)
                    search_end = min(len(lines), i + 5)
                    for j in range(search_start, search_end):
                        # Strip unicode non-breaking spaces etc
                        cleaned = re.sub(r"[^\x20-\x7E]", " ", lines[j]).strip()
                        dm = re.search(date_pattern, cleaned)
                        if dm:
                            try:
                                date_str = dm.group(0)
                                parsed = datetime.strptime(date_str, "%b %d")
                                now = datetime.now()
                                candidate = parsed.replace(year=now.year)
                                if candidate > now:
                                    candidate = candidate.replace(year=now.year - 1)
                                result["posted_date"] = candidate.strftime("%Y-%m-%d")
                            except Exception:
                                pass
                            break

                    # Caption is after "Follow" line
                    if i + 2 < len(lines):
                        cap = lines[i + 2]
                        # Clean date from caption/title if it leaked in
                        if cap:
                            cap = re.sub(date_pattern, "", re.sub(r"[^\x20-\x7E]", " ", cap)).strip()
                        if cap and len(cap) > 5 and not any(c in cap for c in ["󱘺", "ओरिजनल"]):
                            result["title"] = cap.replace("... और", "").replace("... and", "").strip()[:100]
                    break
        except Exception:
            pass

        # Fallback: og:title for views if OCR failed
        if result["views"] is None:
            try:
                og = await page.get_attribute('meta[property="og:title"]', "content")
                if og:
                    result["views"] = _parse_fb_og_views(og)
            except Exception:
                pass

        await browser.close()
        if result["views"] is not None:
            return result
        return {"error": "Could not extract FB view count. Ensure FB cookies are set."}


def _fetch_fb_og_views_from_page(url: str) -> int | None:
    """Fetch og:title directly from FB page HTML and parse views (handles Hindi)."""
    import html as html_mod
    import httpx

    try:
        r = httpx.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }, timeout=15, follow_redirects=True)
        m = re.search(r'<meta property="og:title" content="(.*?)"', r.text)
        if m:
            og = html_mod.unescape(m.group(1))
            return _parse_fb_og_views(og)
    except Exception:
        pass
    return None


def _fetch_fb_ytdlp(url: str) -> dict:
    """Fallback: yt-dlp for Facebook."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-download", "--no-check-certificates", url],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return {"error": result.stderr.strip().split("\n")[-1]}
        meta = json.loads(result.stdout)
        upload = meta.get("upload_date")
        posted = f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" if upload else None
        raw_title = meta.get("title") or ""
        uploader = meta.get("uploader") or ""
        # Try og:title from page first (handles Hindi "2.9 लाख व्यूज़"), then yt-dlp title, then metadata
        views = _fetch_fb_og_views_from_page(url) or _parse_fb_og_views(raw_title) or meta.get("view_count")
        clean = re.sub(r"^[\d.]+[KkMm]?\s*views\s*·\s*[\d.]+[KkMm]?\s*reactions?\s*\|\s*", "", raw_title)
        # Also clean Hindi view patterns from title
        clean = re.sub(r"^[\d.]+\s*(?:लाख|हज़ार|करोड़)\s*(?:व्यूज़?|views)\s*·\s*[\d.]+\s*(?:लाख|हज़ार|करोड़)?\s*(?:reactions?|प्रतिक्रि)\s*\|\s*", "", clean)
        if uploader and clean.endswith(f" | {uploader}"):
            clean = clean[:-(len(uploader) + 3)]
        return {
            "views": views, "likes": meta.get("like_count"), "comments": meta.get("comment_count"),
            "posted_date": posted, "title": clean.strip()[:100], "account": uploader,
        }
    except Exception as e:
        return {"error": str(e)}


def fetch_facebook(url: str) -> dict:
    result = None
    try:
        result = asyncio.run(_fetch_fb_playwright(url))
    except Exception:
        pass

    # If Playwright got views but missing metadata, fill from yt-dlp
    if result and "error" not in result:
        if not result.get("account") or not result.get("title"):
            try:
                ytdlp = _fetch_fb_ytdlp(url)
                if "error" not in ytdlp:
                    if not result.get("account") and ytdlp.get("account"):
                        result["account"] = ytdlp["account"]
                    if not result.get("title") and ytdlp.get("title"):
                        result["title"] = ytdlp["title"]
                    if not result.get("posted_date") and ytdlp.get("posted_date"):
                        result["posted_date"] = ytdlp["posted_date"]
            except Exception:
                pass
        return result

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
