"""SQLite database operations for WatchFinder."""
import sqlite3
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "watches.db"


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sources (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                url         TEXT NOT NULL,
                last_scraped TEXT
            );

            CREATE TABLE IF NOT EXISTS listings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id       INTEGER REFERENCES sources(id),
                title           TEXT NOT NULL,
                brand           TEXT,
                model           TEXT,
                reference       TEXT,
                year            INTEGER,
                price           REAL,
                currency        TEXT DEFAULT 'USD',
                condition       TEXT,
                seller          TEXT,
                listing_url     TEXT UNIQUE,
                description     TEXT,
                image_url       TEXT,
                date_listed     TEXT,
                date_found      TEXT NOT NULL,
                -- Price analysis (only populated for target-year watches)
                price_rating    TEXT CHECK(price_rating IN ('Great','Good','Fair','High',NULL)),
                market_price    REAL,
                price_delta_pct REAL,
                watchcharts_url TEXT,
                is_active       INTEGER DEFAULT 1,
                raw_json        TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_listings_brand  ON listings(brand);
            CREATE INDEX IF NOT EXISTS idx_listings_year   ON listings(year);
            CREATE INDEX IF NOT EXISTS idx_listings_price  ON listings(price);
            CREATE INDEX IF NOT EXISTS idx_listings_rating ON listings(price_rating);
            CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active);

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            INSERT OR IGNORE INTO sources (name, url) VALUES
                ('RolexForums BST',       'https://www.rolexforums.com/forumdisplay.php?f=9'),
                ('Reddit r/Watchexchange','https://www.reddit.com/r/Watchexchange/');
        """)


def upsert_listing(data: dict) -> tuple[int, bool]:
    """Insert or update a listing. Returns (id, is_new)."""
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM listings WHERE listing_url = ?", (data["listing_url"],)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE listings SET
                    title=?, brand=?, model=?, reference=?, year=?,
                    price=?, currency=?, condition=?, seller=?,
                    description=?, image_url=?, date_listed=?,
                    price_rating=?, market_price=?, price_delta_pct=?,
                    watchcharts_url=?, raw_json=?, is_active=1
                WHERE listing_url=?
            """, (
                data.get("title"), data.get("brand"), data.get("model"),
                data.get("reference"), data.get("year"),
                data.get("price"), data.get("currency", "USD"),
                data.get("condition"), data.get("seller"),
                data.get("description"), data.get("image_url"),
                data.get("date_listed"),
                data.get("price_rating"), data.get("market_price"),
                data.get("price_delta_pct"), data.get("watchcharts_url"),
                json.dumps(data.get("extra", {})),
                data["listing_url"],
            ))
            return existing["id"], False
        else:
            cur = conn.execute("""
                INSERT INTO listings (
                    source_id, title, brand, model, reference, year,
                    price, currency, condition, seller, listing_url,
                    description, image_url, date_listed, date_found,
                    price_rating, market_price, price_delta_pct,
                    watchcharts_url, raw_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data.get("source_id"), data.get("title"), data.get("brand"),
                data.get("model"), data.get("reference"), data.get("year"),
                data.get("price"), data.get("currency", "USD"),
                data.get("condition"), data.get("seller"),
                data["listing_url"], data.get("description"),
                data.get("image_url"), data.get("date_listed"), now,
                data.get("price_rating"), data.get("market_price"),
                data.get("price_delta_pct"), data.get("watchcharts_url"),
                json.dumps(data.get("extra", {})),
            ))
            return cur.lastrowid, True


def get_listings(
    brand=None, year=None, year_min=None, year_max=None,
    price_min=None, price_max=None, price_rating=None,
    source=None, condition=None, search=None,
    sort="date_found", order="desc", limit=200, offset=0,
    active_only=True,
):
    clauses = []
    params = []

    if active_only:
        clauses.append("l.is_active = 1")

    if brand:
        clauses.append("LOWER(l.brand) = LOWER(?)")
        params.append(brand)

    if year:
        clauses.append("l.year = ?")
        params.append(int(year))
    else:
        if year_min:
            clauses.append("l.year >= ?")
            params.append(int(year_min))
        if year_max:
            clauses.append("l.year <= ?")
            params.append(int(year_max))

    if price_min is not None:
        clauses.append("l.price >= ?")
        params.append(float(price_min))
    if price_max is not None:
        clauses.append("l.price <= ?")
        params.append(float(price_max))

    if price_rating:
        ratings = price_rating if isinstance(price_rating, list) else [price_rating]
        placeholders = ",".join("?" * len(ratings))
        clauses.append(f"l.price_rating IN ({placeholders})")
        params.extend(ratings)

    if condition:
        clauses.append("LOWER(l.condition) LIKE ?")
        params.append(f"%{condition.lower()}%")

    if source:
        clauses.append("s.name LIKE ?")
        params.append(f"%{source}%")

    if search:
        clauses.append("(l.title LIKE ? OR l.model LIKE ? OR l.reference LIKE ?)")
        term = f"%{search}%"
        params.extend([term, term, term])

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    safe_sort = {
        "date_found": "l.date_found",
        "price": "l.price",
        "year": "l.year",
        "price_rating": "l.price_rating",
        "brand": "l.brand",
        "title": "l.title",
    }.get(sort, "l.date_found")
    safe_order = "ASC" if order.lower() == "asc" else "DESC"

    sql = f"""
        SELECT l.*, s.name AS source_name, s.url AS source_url
        FROM listings l
        LEFT JOIN sources s ON l.source_id = s.id
        {where}
        ORDER BY {safe_sort} {safe_order}
        LIMIT ? OFFSET ?
    """
    params += [limit, offset]

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings WHERE is_active=1").fetchone()[0]
        year_2007 = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE year=2007 AND is_active=1"
        ).fetchone()[0]
        rated = conn.execute(
            "SELECT price_rating, COUNT(*) as cnt FROM listings "
            "WHERE price_rating IS NOT NULL AND is_active=1 GROUP BY price_rating"
        ).fetchall()
        brands = conn.execute(
            "SELECT brand, COUNT(*) as cnt FROM listings "
            "WHERE is_active=1 AND brand IS NOT NULL GROUP BY brand ORDER BY cnt DESC"
        ).fetchall()
        years = conn.execute(
            "SELECT year, COUNT(*) as cnt FROM listings "
            "WHERE is_active=1 AND year IS NOT NULL GROUP BY year ORDER BY year DESC"
        ).fetchall()
        price_stats = conn.execute(
            "SELECT MIN(price), MAX(price), AVG(price) FROM listings "
            "WHERE is_active=1 AND price IS NOT NULL"
        ).fetchone()
        return {
            "total": total,
            "year_2007": year_2007,
            "ratings": {r["price_rating"]: r["cnt"] for r in rated},
            "brands": [{"brand": r["brand"], "count": r["cnt"]} for r in brands],
            "years": [{"year": r["year"], "count": r["cnt"]} for r in years],
            "price_min": price_stats[0],
            "price_max": price_stats[1],
            "price_avg": price_stats[2],
        }


def mark_source_scraped(source_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE sources SET last_scraped=? WHERE id=?",
            (datetime.utcnow().isoformat(), source_id),
        )


def listing_url_exists(url: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM listings WHERE listing_url = ?", (url,)
        ).fetchone()
        return row is not None


def get_source_id(name: str) -> int | None:
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM sources WHERE name=?", (name,)).fetchone()
        return row["id"] if row else None


def get_sources():
    with get_conn() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM sources").fetchall()]


def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value)
        )


def get_all_settings() -> dict:
    with get_conn() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}


if __name__ == "__main__":
    init_db()
    print(f"Database initialised at {DB_PATH}")
