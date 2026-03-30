"""Multi-platform scraper: Instagram, YouTube, Facebook."""

import json
import re
import subprocess
from pathlib import Path

import instaloader
from instaloader.exceptions import (
    BadCredentialsException,
    LoginRequiredException,
    TwoFactorAuthRequiredException,
)

DATA_DIR = Path(__file__).parent / "data"
SESSION_FILE = DATA_DIR / "ig_session"
USERNAME_FILE = DATA_DIR / "ig_username.txt"

# Held between login() and two_factor_login() calls
_pending_login: dict = {"loader": None, "username": ""}


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


def ig_is_logged_in() -> bool:
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
        _pending_login = {"loader": None, "username": ""}
        return {"success": True, "username": username}
    except Exception as e:
        _pending_login = {"loader": None, "username": ""}
        return {"error": str(e)}


def ig_logout():
    SESSION_FILE.unlink(missing_ok=True)
    USERNAME_FILE.unlink(missing_ok=True)


# ── Instagram Fetch ────────────────────────────────────────


def _extract_ig_shortcode(url: str) -> str | None:
    m = re.search(r"instagram\.com/(?:reel|reels|p)/([A-Za-z0-9_-]+)", url)
    return m.group(1) if m else None


def fetch_instagram(url: str) -> dict:
    shortcode = _extract_ig_shortcode(url)
    if not shortcode:
        return {"error": "Invalid Instagram URL"}
    L = _get_ig_loader()
    try:
        post = instaloader.Post.from_shortcode(L.context, shortcode)

        # Instagram shows play_count for reels (not video_view_count)
        views = post.video_view_count
        try:
            node = post._node
            views = node.get("play_count") or node.get("video_play_count") or views
        except Exception:
            pass

        # Handle likes=-1 (Instagram hides like counts sometimes)
        likes = post.likes
        if likes is not None and likes < 0:
            likes = None

        return {
            "views": views,
            "likes": likes,
            "comments": post.comments,
            "posted_date": post.date.strftime("%Y-%m-%d"),
            "title": (post.caption or "")[:100],
            "account": post.owner_username,
        }
    except LoginRequiredException:
        return {"error": "Login required. Connect Instagram in Settings."}
    except Exception as e:
        err = str(e)
        if "401" in err or "403" in err or "login" in err.lower():
            return {"error": "Session expired. Re-login in Settings."}
        return {"error": err}


# ── YouTube Fetch ──────────────────────────────────────────


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
        upload = meta.get("upload_date")  # YYYYMMDD
        posted = f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" if upload else None
        return {
            "views": meta.get("view_count"),
            "likes": meta.get("like_count"),
            "comments": meta.get("comment_count"),
            "posted_date": posted,
            "title": (meta.get("title") or "")[:100],
            "account": meta.get("uploader") or meta.get("channel") or "",
        }
    except subprocess.TimeoutExpired:
        return {"error": "Timeout"}
    except Exception as e:
        return {"error": str(e)}


# ── Facebook Fetch ─────────────────────────────────────────


def _parse_fb_title_views(title: str) -> int | None:
    """Facebook embeds real view count in title: '48K views · 551 reactions | ...'"""
    m = re.match(r"([\d.]+)\s*([KkMm])?\s*views", title)
    if not m:
        return None
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    if suffix == "K":
        num *= 1000
    elif suffix == "M":
        num *= 1_000_000
    return int(num)


def _clean_fb_title(title: str, uploader: str) -> str:
    """Strip 'XXK views · YYY reactions |' prefix and '| Uploader' suffix."""
    clean = re.sub(r"^[\d.]+[KkMm]?\s*views\s*·\s*[\d.]+[KkMm]?\s*reactions\s*\|\s*", "", title)
    if uploader and clean.endswith(f" | {uploader}"):
        clean = clean[: -(len(uploader) + 3)]
    return clean.strip()[:100]


def fetch_facebook(url: str) -> dict:
    try:
        cmd = ["yt-dlp", "--dump-json", "--no-download"]
        cookies = DATA_DIR / "fb_cookies.txt"
        if cookies.exists():
            cmd.extend(["--cookies", str(cookies)])
        cmd.append(url)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            lines = result.stderr.strip().split("\n")
            return {"error": lines[-1] if lines else "Unknown error"}
        meta = json.loads(result.stdout)
        upload = meta.get("upload_date")
        posted = f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" if upload else None

        raw_title = meta.get("title") or ""
        uploader = meta.get("uploader") or ""

        # Facebook API view_count is wrong — parse from title instead
        views = _parse_fb_title_views(raw_title) or meta.get("view_count")
        title = _clean_fb_title(raw_title, uploader) if raw_title else ""

        return {
            "views": views,
            "likes": meta.get("like_count"),
            "comments": meta.get("comment_count"),
            "posted_date": posted,
            "title": title,
            "account": uploader,
        }
    except subprocess.TimeoutExpired:
        return {"error": "Timeout"}
    except Exception as e:
        return {"error": str(e)}


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
