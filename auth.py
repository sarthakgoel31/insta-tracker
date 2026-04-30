"""JWT authentication middleware for Supabase Auth."""

import os
import httpx
import json

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://qutgezcgynqxqdtcgmfz.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "sb_publishable_ffeLPBFV2ZlnqF_2RMk7Jg_gVn_LBGH")

TRIAL_LIMIT = 5  # Max URLs for anonymous users


async def verify_supabase_token(token: str) -> dict:
    """Verify token with Supabase GoTrue and return user info."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": SUPABASE_ANON_KEY,
            },
        )
    if resp.status_code != 200:
        raise HTTPException(401, "Invalid or expired token")
    return resp.json()


class AuthMiddleware(BaseHTTPMiddleware):
    """Extract user from Authorization header. Allow anonymous for trial mode."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip auth for public routes and static files
        if path == "/" or path.startswith("/static") or path == "/api/health":
            return await call_next(request)

        if path.startswith("/api/"):
            auth_header = request.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                try:
                    user = await verify_supabase_token(token)
                    request.state.user_id = user["id"]
                    request.state.user_email = user.get("email", "")
                    request.state.is_anonymous = False
                except HTTPException:
                    raise
                except Exception as e:
                    raise HTTPException(401, f"Auth failed: {e}")
            else:
                # Anonymous trial mode
                request.state.user_id = None
                request.state.user_email = ""
                request.state.is_anonymous = True

        return await call_next(request)


def get_user_id(request: Request) -> str:
    """Extract user_id from request state. Returns None for anonymous."""
    return getattr(request.state, "user_id", None)


def is_anonymous(request: Request) -> bool:
    return getattr(request.state, "is_anonymous", True)


def require_auth(request: Request) -> str:
    """Require authenticated user. Raises 401 for anonymous."""
    user_id = get_user_id(request)
    if not user_id:
        raise HTTPException(401, "Please sign up to continue. Free accounts can track unlimited YouTube & Facebook URLs.")
    return user_id
