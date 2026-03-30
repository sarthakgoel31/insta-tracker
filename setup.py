#!/usr/bin/env python3
"""One-time Instagram login — saves session for the tracker."""

import instaloader
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
SESSION_FILE = DATA_DIR / "ig_session"
USERNAME_FILE = DATA_DIR / "ig_username.txt"


def setup():
    print("Instagram Login Setup")
    print("=" * 40)
    username = input("Instagram username: ").strip()
    if not username:
        print("Cancelled.")
        return

    L = instaloader.Instaloader()
    try:
        L.interactive_login(username)
        DATA_DIR.mkdir(exist_ok=True)
        L.save_session_to_file(str(SESSION_FILE))
        USERNAME_FILE.write_text(username)
        print(f"\nLogged in as @{username}. Session saved.")
        print("You can now start the tracker: python3 server.py")
    except Exception as e:
        print(f"\nLogin failed: {e}")


if __name__ == "__main__":
    setup()
