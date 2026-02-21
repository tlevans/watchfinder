"""
WatchFinder — CLI entry point.

Usage:
  uv run main.py            # start web server (default port 5000)
  uv run main.py --scrape   # run scraper once, then exit
  uv run main.py --init-db  # initialise DB only
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main():
    parser = argparse.ArgumentParser(description="WatchFinder watch listing tracker")
    parser.add_argument("--scrape", action="store_true", help="Run scraper once and exit")
    parser.add_argument("--init-db", action="store_true", help="Initialise database and exit")
    parser.add_argument("--pages", type=int, default=3, help="Pages to scrape (default 3)")
    parser.add_argument("--target-year", type=int, default=2007, help="Birth year to check prices for")
    parser.add_argument("--port", type=int, default=5000, help="Web server port (default 5000)")
    parser.add_argument("--host", default="0.0.0.0", help="Web server host (default 0.0.0.0)")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    args = parser.parse_args()

    import database as db
    db.init_db()

    if args.init_db:
        print("Database initialised.")
        return

    if args.scrape:
        import scraper as sc
        print(f"Starting scrape ({args.pages} pages, target year {args.target_year})…")
        stats = sc.run_all_scrapers(pages=args.pages, target_year=args.target_year)
        print("Done:", stats)
        return

    # Default: start web server
    from app import app
    print(f"WatchFinder starting on http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
