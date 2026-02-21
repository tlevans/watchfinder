# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Start web server (http://localhost:5000)
uv run main.py

# Start with options
uv run main.py --port 8080 --debug

# Run scraper from CLI (without starting server)
uv run main.py --scrape --pages 5 --target-year 2007

# Initialise DB only
uv run main.py --init-db

# Seed 10 demo listings
uv run python seed_demo.py
```

No test suite exists currently.

## Architecture

The app is a Flask SPA with SQLite persistence. All Python modules are flat at the project root.

**Request flow for scraping:**
1. `POST /api/scrape` → `app.py` spawns a daemon thread
2. Thread calls `scraper.run_all_scrapers()` with per-source cookie strings loaded from DB
3. Each scraper (`RolexForumsScraper`, `RedditWatchexchangeScraper`) fetches pages, parses HTML/JSON into listing dicts
4. For listings matching `target_year`, `scraper.enrich_with_price()` calls `price_checker.check_price()`
5. `price_checker` hits WatchCharts (search API → chart page → marketplace median fallback) to get market price, then calls `rate_price()` to produce `Great/Good/Fair/High`
6. Listings are upserted via `db.upsert_listing()` (keyed on `listing_url UNIQUE`)
7. Frontend polls `GET /api/scrape/status` until `running=False`

**Data flow for the frontend:**
- `GET /api/listings` with filter params → `db.get_listings()` (parameterised SQL, whitelist-validated sort column) → JSON array
- `static/app.js` renders cards/rows; no JS framework

## Key Design Constraints

- **No Cloudflare bypass**: RolexForums uses Cloudflare Bot Management. The scraper does NOT attempt stealth/bypass — it detects the 403 "Just a moment" response and logs a clear message. Users must run locally on residential IPs with session cookies pasted via Settings UI.
- **Cookies stored in DB**: `settings` table with key `cookie_{source_name}`. On `GET /api/settings` cookie values are redacted to `***`. Only keys with prefix `cookie_`, `target_year`, or `scrape_pages` are accepted on POST.
- **Upsert key**: `listing_url` is the deduplication key. Manual imports without a URL get a `manual://` + md5 synthetic URL.
- **Price rating only for target year**: `enrich_with_price()` skips listings where `year != target_year` or price/brand is missing.
- **Sort injection prevention**: `db.get_listings()` maps sort params through a whitelist dict before interpolating into SQL.

## Adding a New Scraper

1. Subclass `BaseScraper` in `scraper.py`, set `SOURCE_NAME` to match the DB sources table
2. Implement `run(pages, target_year) -> dict` with keys: `source, pages, threads, new, updated, priced, errors, blocked`
3. Call `enrich_with_price(listing, target_year)` before `db.upsert_listing(listing)`
4. Register in `run_all_scrapers()` dict and add an `INSERT OR IGNORE INTO sources` row in `database.py:init_db()`
