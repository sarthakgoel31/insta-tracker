<p align="center">
  <a href="https://social-tracker.sarthakgoel.cv"><img src="https://img.shields.io/badge/demo-live-brightgreen" alt="Live Demo" /></a>
  <img src="https://img.shields.io/badge/python-3-3776AB?logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Supabase-3FCF8E?logo=supabase&logoColor=white" alt="Supabase" />
</p>

# Social Branding Tracker

Multi-tenant SaaS that tracks YouTube, Facebook, and Instagram reel views with monthly cohort analysis. Free trial, no login required.

**Live:** [social-tracker.sarthakgoel.cv](https://social-tracker.sarthakgoel.cv)

## Why

Social media managers manually count reel views in spreadsheets every week. This tool scrapes view counts automatically across 3 platforms, tracks them over time, and generates cohort analysis to measure content performance trends.

## How

```
Paste URL → Auto-detect platform → Scrape view count
         → Track over time → Monthly cohort analysis
```

1. Paste any YouTube, Facebook, or Instagram URL
2. Views are fetched automatically (Invidious API for YT, Playwright for FB)
3. Refresh to update view counts over time
4. Cohort analysis shows M0/M1/M2 growth by posting month

## Tiers

| Tier | URLs | Platforms | Auth |
|---|---|---|---|
| Trial | 5 | YouTube + Facebook | None (no login) |
| Free | 20 | YouTube + Facebook | Email/password |
| Paid | Unlimited | YouTube + Facebook + Instagram | [Book a call](https://cal.com/sarthakgoel31) |

## Features

| Feature | Description |
|---|---|
| Multi-Platform | YouTube (Invidious API), Facebook (Playwright), Instagram (GraphQL + cookies) |
| Trial Mode | 5 URLs without login, instant scraping |
| Monthly Cohort Analysis | M0/M1/M2 view growth grouped by posting month |
| Pivot Tables | Group by account, platform, or month |
| Bulk Add | Paste multiple URLs at once |
| Google Sheets Export | Push all data to Google Sheets |
| Daily Auto-Refresh | 8 AM IST cron refreshes all users' reels |
| Per-User Data | Supabase Auth + RLS, each user sees only their data |

## Tech

| Component | Technology |
|---|---|
| Backend | Python 3, FastAPI, Uvicorn |
| Database | Supabase Postgres (multi-tenant with RLS) |
| Auth | Supabase Auth (email/password) |
| YouTube | Invidious API (free, no cookies needed) |
| Facebook | Playwright headless Chromium |
| Instagram | GraphQL API + Instaloader (paid tier only) |
| Hosting | Render free tier |
| Frontend | Static HTML served by FastAPI |

## Architecture

```
insta-tracker/
  server.py              # FastAPI server — all routes + async pipeline
  auth.py                # JWT middleware for Supabase Auth
  db.py                  # Supabase Postgres client (replaces SQLite)
  scraper.py             # Multi-platform scraper (IG/YT/FB)
  static/index.html      # Dashboard UI (login, trial, tier gating)
  supabase_schema.sql    # Postgres schema + RLS policies
```

## Status

| Item | Status |
|---|---|
| Multi-platform scraping | Complete |
| Supabase Auth + per-user data | Complete |
| Trial mode (5 URLs, no login) | Complete |
| Free tier (20 URLs) | Complete |
| Paid tier (Instagram) | Complete |
| Monthly cohort analysis | Complete |
| Google Sheets export | Complete |
| Daily auto-refresh cron | Complete |
| Render deployment | Complete |
| Custom domain | Complete |

---

Built by [Sarthak Goel](https://sarthakgoel.cv)
