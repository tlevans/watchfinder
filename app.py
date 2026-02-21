"""
WatchFinder Flask web application.
"""
import json
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify, render_template, request, abort
from flask_cors import CORS

import database as db
import scraper as sc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ── DB init ───────────────────────────────────────────────────────────────────
db.init_db()

# ── Scrape state ──────────────────────────────────────────────────────────────
_scrape_lock = threading.Lock()
_scrape_status = {"running": False, "last_run": None, "last_result": None, "progress": None}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_cookies() -> dict[str, str]:
    """Load per-source cookie strings from settings."""
    cookies = {}
    for source in db.get_sources():
        key = f"cookie_{source['name']}"
        val = db.get_setting(key, "")
        if val:
            cookies[source["name"]] = val
    return cookies


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/listings")
def api_listings():
    p = request.args
    rows = db.get_listings(
        brand=p.get("brand") or None,
        year=p.get("year") or None,
        year_min=p.get("year_min") or None,
        year_max=p.get("year_max") or None,
        price_min=p.get("price_min") or None,
        price_max=p.get("price_max") or None,
        price_rating=p.getlist("rating") or None,
        source=p.get("source") or None,
        condition=p.get("condition") or None,
        search=p.get("q") or None,
        sort=p.get("sort", "date_found"),
        order=p.get("order", "desc"),
        limit=int(p.get("limit", 200)),
        offset=int(p.get("offset", 0)),
    )
    return jsonify({"listings": rows, "count": len(rows)})


@app.route("/api/stats")
def api_stats():
    return jsonify(db.get_stats())


@app.route("/api/sources")
def api_sources():
    return jsonify(db.get_sources())


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    settings = db.get_all_settings()
    # Redact cookie values for security (show presence only)
    safe = {}
    for k, v in settings.items():
        if k.startswith("cookie_"):
            safe[k] = "***" if v else ""
        else:
            safe[k] = v
    return jsonify(safe)


@app.route("/api/settings", methods=["POST"])
def api_settings_post():
    data = request.json or {}
    allowed_prefixes = ("cookie_", "target_year", "scrape_pages")
    for key, value in data.items():
        if any(key.startswith(p) for p in allowed_prefixes):
            db.set_setting(key, str(value))
    return jsonify({"ok": True})


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    if _scrape_lock.locked():
        return jsonify({"error": "Scrape already in progress"}), 409

    body = request.json or {}
    pages       = int(body.get("pages", db.get_setting("scrape_pages", "3")))
    target_year = int(body.get("target_year", db.get_setting("target_year", "2007")))
    sources_req = body.get("sources") or None  # list of source names, or None for all

    def _run():
        global _scrape_status
        _scrape_status["running"] = True
        _scrape_status["progress"] = None
        try:
            cookies = _load_cookies()
            result  = sc.run_all_scrapers(
                pages=pages,
                target_year=target_year,
                cookies=cookies,
                sources=sources_req,
                progress_callback=lambda p: _scrape_status.update({"progress": p}),
            )
            _scrape_status["last_result"] = result
            _scrape_status["last_run"] = datetime.utcnow().isoformat()
        except Exception as e:
            log.exception("Scrape failed: %s", e)
            _scrape_status["last_result"] = {"error": str(e)}
        finally:
            _scrape_status["running"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({"status": "started", "pages": pages, "target_year": target_year})


@app.route("/api/scrape/status")
def api_scrape_status():
    return jsonify({**_scrape_status})


@app.route("/api/listing/<int:listing_id>")
def api_listing_detail(listing_id):
    with db.get_conn() as conn:
        row = conn.execute(
            """SELECT l.*, s.name AS source_name FROM listings l
               LEFT JOIN sources s ON l.source_id = s.id
               WHERE l.id = ?""",
            (listing_id,),
        ).fetchone()
    if not row:
        abort(404)
    return jsonify(dict(row))



@app.route("/api/import", methods=["POST"])
def api_import():
    """Manually import one or more watch listings as JSON."""
    data = request.json
    if not data:
        return jsonify({"error": "No data"}), 400

    listings = data if isinstance(data, list) else [data]
    added = updated = errors = 0

    for item in listings:
        try:
            if "listing_url" not in item:
                import hashlib, time as t
                item["listing_url"] = (
                    "manual://" + hashlib.md5(
                        (str(t.time()) + str(item)).encode()
                    ).hexdigest()[:12]
                )
            source = db.get_sources()
            item.setdefault("source_id", source[0]["id"] if source else None)
            _, is_new = db.upsert_listing(item)
            if is_new:
                added += 1
            else:
                updated += 1
        except Exception as e:
            log.exception("Import error: %s", e)
            errors += 1

    return jsonify({"added": added, "updated": updated, "errors": errors})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
