# WatchFinder

**Birth Year Watch Tracker** — scrapes watch forum listings, identifies birth-year watches (e.g., 2007), and rates them as Great / Good / Fair / High using market data from WatchCharts.

## Features

- **Forum scraping** — RolexForums BST + Reddit r/Watchexchange (extensible)
- **Auto price rating** — for watches produced in your target year, fetches market price from WatchCharts and rates the listing:
  - **Great** <= 90% of market price
  - **Good** 90-100% of market
  - **Fair** 100-110% of market
  - **High** > 110% of market
- **SQLite storage** — all listings persisted locally; re-scrapes update existing records
- **Rich frontend** — filter by brand, year, price, deal rating, condition, source; grid & list views
- **Cookie auth** — paste your forum session cookies to authenticate with Cloudflare-protected forums
- **Manual import** — POST JSON to `/api/import` to add listings directly

## Quick Start

```bash
# Install dependencies
uv sync

# Seed demo data (optional - 10 realistic listings pre-loaded)
uv run python seed_demo.py

# Start the web server (default: http://localhost:5000)
uv run main.py

# Or with options:
uv run main.py --port 8080

# Run scraper once from command line:
uv run main.py --scrape --pages 5 --target-year 2007
```

## Forum Scraping Notes

### Cloudflare Protection
RolexForums uses Cloudflare Bot Management. To scrape it:
1. **Run WatchFinder locally** on your home network (residential IPs are not blocked)
2. **Log in** to RolexForums in your browser
3. Open DevTools -> Application -> Cookies -> www.rolexforums.com
4. Copy all cookies as a string: `name1=val1; name2=val2; ...`
5. Paste into **Settings -> Forum Session Cookies** in the app

Reddit r/Watchexchange may also block cloud server IPs; run locally for best results.

## Architecture

```
watchfinder/
├── app.py           Flask web application
├── database.py      SQLite operations
├── scraper.py       Forum scrapers (RolexForums, Reddit)
├── price_checker.py WatchCharts market price lookup
├── seed_demo.py     Demo data (10 realistic listings)
├── main.py          CLI entry point
├── templates/
│   └── index.html   Single-page frontend
├── static/
│   ├── style.css    Dark luxury theme
│   └── app.js       Vanilla JS frontend
└── watches.db       SQLite database (auto-created)
```

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/listings` | Filtered listings |
| GET | `/api/stats` | Dashboard stats |
| GET | `/api/sources` | Scrape sources |
| POST | `/api/scrape` | Trigger scrape |
| GET | `/api/scrape/status` | Scrape progress |
| GET/POST | `/api/settings` | Settings and cookies |
| POST | `/api/import` | Manual listing import |

### Listing filters
`brand`, `year`, `year_min`, `year_max`, `price_min`, `price_max`, `rating` (repeatable), `condition`, `source`, `q` (search), `sort`, `order`
