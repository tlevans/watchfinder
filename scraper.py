"""
Forum scraper for watch listings.

Currently supports:
  - RolexForums Buy/Sell/Trade  (vBulletin, Cloudflare-protected)
  - Reddit r/Watchexchange       (JSON API)
  - Manual import               (paste listing data)

NOTE: RolexForums and many other watch forums are protected by Cloudflare
Bot Management. Scraping works best when:
  1. Running on a residential IP (run the app locally, not on a cloud server)
  2. Using an authenticated session (provide cookies via the Settings UI)
"""
import hashlib
import mimetypes
import os
import re
import time
import logging
import threading
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, quote_plus

import database as db
import price_checker

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

SCRAPE_DO_KEY = os.environ.get("SCRAPE_DO_API_KEY", "")

IMAGE_DIR = Path(__file__).parent / "static" / "images"

log = logging.getLogger(__name__)

TARGET_YEAR = 2007  # birth year — trigger price analysis for this year

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# ──────────────────────────────────────────────────────────────────────────────
#  Regex helpers
# ──────────────────────────────────────────────────────────────────────────────

YEAR_RE = re.compile(r"\b(19[5-9]\d|200[0-9]|201[0-9]|202[0-5])\b")
PRICE_RE = re.compile(
    r"\$\s*([\d,]+(?:\.\d{1,2})?)"                                     # $1,234.00
    r"|([\d,]+(?:\.\d{1,2})?)\s*USD"                                   # 1234 USD
    r"|([\d,.]+)\s*k(?!\s*\/?\s*(?:YG|WG|RG|SS|gold|white|yellow|rose|ct|carat|karat))\b" # 8.5k / 8k (not "18k gold/YG/WG/RG/SS" or "18K/SS")
    r"|USD\s*([\d,]+(?:\.\d{1,2})?)"                                   # USD 1234
    r"|(?:asking|priced?\s*(?:at|:)?)\s*\$?\s*([\d,]+(?:\.\d{1,2})?)" # asking/price 8500
    r"|([\d,]+(?:\.\d{1,2})?)\s*(?:obo|shipped|firm|tyd)\b",          # 8500 OBO/shipped
    re.IGNORECASE,
)
REFERENCE_RE = re.compile(
    r"\b(1\d{4,5}[A-Z]{0,3}"               # Rolex 5-6 digit ± suffix
    r"|PAM\s?\d{3,4}"                       # Panerai
    r"|IW\s?\d{6}"                          # IWC
    r"|15[234]\d{2}[A-Z]?"                  # AP Royal Oak
    r"|5[0-9]{3}[A-Z0-9]{1,4}"             # Patek
    r"|\d{4}[A-Z]{1,3}[\d.]*"              # generic
    r")\b",
    re.IGNORECASE,
)

BRAND_ALIASES = {
    # Long strings first (avoid partial matches)
    "audemars piguet": "Audemars Piguet",
    "patek philippe": "Patek Philippe",
    "vacheron constantin": "Vacheron Constantin",
    "grand seiko": "Grand Seiko",
    "tag heuer": "TAG Heuer",
    "jaeger-lecoultre": "Jaeger-LeCoultre",
    "jaeger lecoultre": "Jaeger-LeCoultre",
    "a. lange": "A. Lange & Söhne",
    "lange & sohne": "A. Lange & Söhne",
    "gmt-master": "Rolex",   # model → brand
    "gmt master": "Rolex",
    "datejust": "Rolex",
    "submariner": "Rolex",
    "daytona": "Rolex",
    "sea-dweller": "Rolex",
    "sky-dweller": "Rolex",
    "milgauss": "Rolex",
    "yacht-master": "Rolex",
    "day-date": "Rolex",
    "air-king": "Rolex",
    "explorer": "Rolex",
    "rolex": "Rolex",
    "tudor": "Tudor",
    "omega": "Omega",
    "breitling": "Breitling",
    "navitimer": "Breitling",
    "panerai": "Panerai",
    "luminor": "Panerai",
    "radiomir": "Panerai",
    "cartier": "Cartier",
    "blancpain": "Blancpain",
    "zenith": "Zenith",
    "hublot": "Hublot",
    "patek": "Patek Philippe",
    "seiko": "Seiko",
    "audemars": "Audemars Piguet",
    "royal oak": "Audemars Piguet",
    "vacheron": "Vacheron Constantin",
    "iwc": "IWC",
    "jaeger": "Jaeger-LeCoultre",
    "jlc": "Jaeger-LeCoultre",
    "lange": "A. Lange & Söhne",
}

ROLEX_MODELS = {
    "datejust": "Datejust",
    "submariner": "Submariner",
    "daytona": "Daytona",
    "gmt-master ii": "GMT-Master II",
    "gmt-master": "GMT-Master II",
    "gmt master": "GMT-Master II",
    "explorer ii": "Explorer II",
    "explorer": "Explorer",
    "air-king": "Air-King",
    "milgauss": "Milgauss",
    "sea-dweller": "Sea-Dweller",
    "sky-dweller": "Sky-Dweller",
    "yacht-master": "Yacht-Master",
    "day-date": "Day-Date",
    "president": "Day-Date",
    "cellini": "Cellini",
    "pearlmaster": "Pearlmaster",
    "oysterquartz": "Oysterquartz",
}

CONDITION_MAP = {
    "new/unworn": "New/Unworn",
    "new unworn": "New/Unworn",
    "unworn": "New/Unworn",
    "brand new": "New/Unworn",
    "mint": "Mint",
    "excellent": "Excellent",
    "very good": "Very Good",
    "good": "Good",
    "fair": "Fair",
}


def extract_price(text: str) -> float | None:
    for m in PRICE_RE.finditer(text):
        # Group 3 is the explicit k-multiplier group (e.g. "8.5k")
        is_k = m.group(3) is not None
        raw = next((g for g in m.groups() if g is not None), None)
        if raw is None:
            continue
        # If a non-k pattern matched (e.g. "asking 8.5"), check whether the
        # captured number is immediately followed by a 'k' multiplier that the
        # pattern didn't consume (e.g. "asking 8.5k obo").
        if not is_k:
            end = m.end()
            if end < len(text) and text[end].lower() == "k":
                after_k = text[end + 1 : end + 9].lstrip("/").lstrip()
                if not re.match(r"YG|WG|RG|SS|gold|white|yellow|rose|ct|carat|karat", after_k, re.I):
                    is_k = True
        # "5,5k" or "3,8k" → European decimal comma (5.5k / 3.8k = $5500 / $3800)
        # "5,500k" or "1,234" → American thousands separator (keep as-is)
        if is_k and re.match(r'^\d+,\d{1,2}$', raw):
            raw = raw.replace(",", ".")
        else:
            raw = raw.replace(",", "")
        try:
            val = float(raw)
            if is_k:
                val *= 1000
            if 100 <= val <= 500_000:
                return round(val, 2)
        except (ValueError, AttributeError):
            pass
    return None


def extract_year(text: str) -> int | None:
    for y in YEAR_RE.findall(text):
        y = int(y)
        if 1950 <= y <= 2025:
            return y
    return None


def extract_reference(text: str) -> str | None:
    m = REFERENCE_RE.search(text)
    return m.group(0).strip() if m else None


def extract_brand(text: str) -> str | None:
    """Return the brand whose alias keyword appears earliest in the text.

    Sorting by key length (longest-first) was wrong: e.g. "royal oak" (9)
    matched before "datejust" (8) causing Rolex listings that mention a Royal
    Oak trade to be labelled as Audemars Piguet.  Using leftmost position
    means the primary subject of a title wins over incidental mentions.
    """
    lower = text.lower()
    best_pos = len(lower)
    best_brand = None
    for key, val in BRAND_ALIASES.items():
        pos = lower.find(key)
        if pos != -1 and pos < best_pos:
            best_pos = pos
            best_brand = val
    return best_brand


def extract_model(text: str, brand: str | None = None) -> str | None:
    lower = text.lower()
    if brand and "rolex" in brand.lower():
        for key, val in sorted(ROLEX_MODELS.items(), key=lambda x: -len(x[0])):
            if key in lower:
                return val
    return None


def extract_condition(text: str) -> str | None:
    lower = text.lower()
    for phrase, label in sorted(CONDITION_MAP.items(), key=lambda x: -len(x[0])):
        if phrase in lower:
            return label
    return None


def is_for_sale(title: str) -> bool:
    lower = title.lower()
    return not re.search(r"\b(wtb|iso|in search of|want to buy|wantto buy)\b", lower)


def enrich_with_price(listing: dict, target_year: int) -> dict:
    """Optionally call WatchCharts to rate the listing price."""
    if (
        listing.get("year") == target_year
        and listing.get("price")
        and listing.get("brand")
    ):
        try:
            pc = price_checker.check_price(
                brand=listing["brand"],
                model=listing.get("model", ""),
                reference=listing.get("reference", ""),
                listing_price=listing["price"],
                year=listing.get("year"),
            )
            listing["price_rating"] = pc["rating"]
            listing["market_price"] = pc["market_price"]
            listing["price_delta_pct"] = pc["pct_of_market"]
            listing["watchcharts_url"] = pc["watchcharts_url"]
        except Exception as e:
            log.warning("Price check failed: %s", e)
    return listing


# ──────────────────────────────────────────────────────────────────────────────
#  Base scraper
# ──────────────────────────────────────────────────────────────────────────────

class BaseScraper:
    SOURCE_NAME: str = ""

    def __init__(self, cookie_string: str = "", delay: float = 2.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(BASE_HEADERS)
        if cookie_string:
            # Parse "name=value; name2=value2" cookie string
            for pair in cookie_string.split(";"):
                pair = pair.strip()
                if "=" in pair:
                    name, _, value = pair.partition("=")
                    self.session.cookies.set(name.strip(), value.strip())
        self.source_id = db.get_source_id(self.SOURCE_NAME)

    def _get(self, url: str, **kwargs) -> requests.Response | None:
        try:
            if SCRAPE_DO_KEY:
                # Route through Scrape.do — handles Cloudflare and rotating proxies
                cookie_header = "; ".join(
                    f"{k}={v}" for k, v in self.session.cookies.items()
                )
                # Forward all session headers (User-Agent, Accept, etc.) so the
                # target site sees them correctly (e.g. Reddit needs Accept: application/json)
                headers = dict(self.session.headers)
                if cookie_header:
                    headers["Cookie"] = cookie_header
                for attempt in range(2):
                    resp = requests.get(
                        "https://api.scrape.do/",
                        params={
                            "token": SCRAPE_DO_KEY,
                            "url": url,
                            "forwardHeaders": "true",
                        },
                        headers=headers,
                        timeout=60,
                    )
                    if resp.status_code != 502:
                        break
                    log.warning("Scrape.do 502 on attempt %d, retrying…", attempt + 1)
                    time.sleep(3)
            else:
                resp = self.session.get(url, timeout=20, **kwargs)
                if resp.status_code == 403 and "Just a moment" in resp.text:
                    log.warning(
                        "Cloudflare Bot Management detected at %s. "
                        "Please run WatchFinder on a residential network and provide "
                        "your forum session cookies via Settings.",
                        url,
                    )
                    return None
            resp.raise_for_status()
            return resp
        except requests.Timeout:
            log.warning("Timeout fetching %s", url)
        except requests.HTTPError as e:
            # Avoid logging Scrape.do URL which contains the API token
            status = e.response.status_code if e.response is not None else "?"
            log.warning("HTTP %s fetching %s", status, url)
        except requests.RequestException as e:
            log.warning("Request error fetching %s: %s", url, e)
        return None

    def _fetch_image(self, url: str) -> str | None:
        """Download an image and cache it locally.

        CDN-hosted images (Imgur, Google Photos, Postimg, etc.) are fetched
        directly — Scrape.do's headless browser can't return raw image binaries
        and returns 502 for i.imgur.com URLs.  Forum attachments (attachment.php)
        fall back to _get() so they benefit from session cookies / Scrape.do.
        """
        IMAGE_DIR.mkdir(parents=True, exist_ok=True)
        url_hash = hashlib.md5(url.encode()).hexdigest()
        for ext in (".jpg", ".png", ".webp", ".gif"):
            if (IMAGE_DIR / (url_hash + ext)).exists():
                return f"/static/images/{url_hash}{ext}"

        resp = None

        # Try a direct HTTP fetch first (no Scrape.do).  This works for all
        # public CDN image hosts and avoids the Scrape.do 502 on Imgur.
        try:
            direct = requests.get(
                url,
                headers={
                    "User-Agent": BASE_HEADERS["User-Agent"],
                    "Referer": "https://www.rolexforums.com/",
                    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                },
                timeout=20,
                allow_redirects=True,
            )
            ct = direct.headers.get("Content-Type", "")
            if direct.status_code == 200 and ct.startswith("image/"):
                resp = direct
        except Exception:
            pass

        # Fall back to _get() (Scrape.do-aware) only for forum-hosted attachments.
        # External CDN URLs (Imgur etc.) cause Scrape.do 502s — skip the fallback
        # for those and go straight to the browser-URL fallback below.
        is_forum_attachment = "attachment.php" in url or (
            not url.startswith("http") or
            any(h in url for h in ("rolexforums.com", "watchuseek.com", "timezone.com"))
        )
        if resp is None and is_forum_attachment:
            resp = self._get(url)

        if resp:
            try:
                content_type = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
                if content_type.startswith("image/"):
                    ext = mimetypes.guess_extension(content_type) or ".jpg"
                    ext = {".jpe": ".jpg", ".jpeg": ".jpg"}.get(ext, ext)
                    if ext not in (".jpg", ".png", ".webp", ".gif"):
                        ext = ".jpg"
                    filepath = IMAGE_DIR / (url_hash + ext)
                    filepath.write_bytes(resp.content)
                    log.info("Image cached: %s → %s", url[:60], filepath.name)
                    return f"/static/images/{url_hash}{ext}"
                else:
                    log.warning("Non-image response (%s) for %s", content_type, url[:80])
            except Exception as e:
                log.warning("Image download failed for %s: %s", url[:60], e)

        # Server-side download failed (cloud IP blocked by CDN, Scrape.do 502, etc.).
        # Return the original URL so the browser can fetch it directly — this works
        # for public CDN hosts (Imgur, Google Photos, Postimg) which are accessible
        # from user browsers but not cloud server IPs.
        if url.startswith("http") and "attachment.php" not in url:
            log.info("Falling back to original URL for browser-side load: %s", url[:80])
            return url
        return None

    def run(self, pages: int = 3, target_year: int = TARGET_YEAR) -> dict:
        raise NotImplementedError


# ──────────────────────────────────────────────────────────────────────────────
#  RolexForums scraper
# ──────────────────────────────────────────────────────────────────────────────

class RolexForumsScraper(BaseScraper):
    SOURCE_NAME = "RolexForums BST"
    BASE = "https://www.rolexforums.com"
    FORUM_URL = "https://www.rolexforums.com/forumdisplay.php?f=9"

    def _parse_forum_page(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        threads = []

        # vBulletin 4 style: <a id="thread_title_XXXXX">
        for a in soup.find_all("a", id=re.compile(r"^thread_title_")):
            href = a.get("href", "")
            if href:
                threads.append({
                    "title": a.get_text(strip=True),
                    "url": urljoin(self.BASE, href),
                    "seller": None,
                    "date_str": None,
                })

        # vBulletin 5 style
        for li in soup.select("li.threadbit, div.threadbit"):
            a = li.find("a", class_=re.compile(r"title|thread")) or li.find(
                "a", href=re.compile(r"showthread")
            )
            if not a:
                continue
            seller_tag = li.find(class_=re.compile(r"username|author"))
            date_tag   = li.find(class_=re.compile(r"date|time"))
            threads.append({
                "title": a.get_text(strip=True),
                "url": urljoin(self.BASE, a.get("href", "")),
                "seller": seller_tag.get_text(strip=True) if seller_tag else None,
                "date_str": date_tag.get_text(strip=True) if date_tag else None,
            })

        # Generic fallback
        if not threads:
            for a in soup.select("a.title, a.threadtitle"):
                if "showthread" in a.get("href", ""):
                    threads.append({
                        "title": a.get_text(strip=True),
                        "url": urljoin(self.BASE, a["href"]),
                        "seller": None,
                        "date_str": None,
                    })

        return threads

    def _parse_thread(self, url: str, stub: dict) -> dict | None:
        resp = self._get(url)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "lxml")

        # ── Locate first post body ────────────────────────────────────────────
        # vBulletin 3.x (used by RolexForums): <table id="post{ID}"> wraps each
        # post; the text lives in <div id="post_message_{ID}"> and the full cell
        # (including attachments) is <td id="td_post_{ID}">.
        post = None
        td_post = None
        first_post_table = soup.find("table", id=re.compile(r"^post\d+"))
        if first_post_table:
            m = re.search(r"\d+", first_post_table["id"])
            if m:
                pid = m.group()
                post    = soup.find("div", id=f"post_message_{pid}")
                td_post = soup.find("td",  id=f"td_post_{pid}")

        # vBulletin 4 / other forums fallback
        if not post:
            post = (
                soup.find("blockquote", class_=re.compile(r"\bpostcontent\b"))
                or soup.find("div", class_=re.compile(r"\bpostbody\b"))
                or soup.find("div", class_=re.compile(r"\bpost-content\b"))
            )

        body = post.get_text(" ", strip=True) if post else ""
        combined = f"{stub['title']} {body}"

        # ── Find first real listing photo ─────────────────────────────────────
        # For vB3 search the whole td_post (includes forum attachments that live
        # outside post_message); fall back to the post div then the whole page.
        image_url = None
        search_scope = []
        if td_post:
            search_scope = td_post.find_all("img")
        if not search_scope and first_post_table:
            search_scope = first_post_table.find_all("img")
        if not search_scope:
            search_scope = soup.find_all("img")
        for img in search_scope:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            if not src:
                continue
            if re.search(r"smilie|smiley|icon|avatar|emoji|pixel|spacer|logo|btn|button", src, re.I):
                continue
            try:
                w = img.get("width", "")
                h = img.get("height", "")
                if (w and int(w) < 80) or (h and int(h) < 80):
                    continue
            except (ValueError, TypeError):
                pass  # non-integer attribute like "100%" — don't skip
            if src.startswith("http"):
                pass  # already absolute
            elif src.startswith("/"):
                src = urljoin(self.BASE, src)
            elif src.startswith("attachment.php"):
                # vBulletin forum attachment — make absolute
                src = self.BASE + "/" + src
            else:
                continue
            image_url = src
            break

        if image_url:
            image_url = self._fetch_image(image_url)

        seller = stub.get("seller")
        if not seller:
            at = soup.find(class_=re.compile(r"username|author"))
            if at:
                seller = at.get_text(strip=True)

        brand = extract_brand(stub['title']) or extract_brand(combined)
        price = extract_price(combined)
        year  = extract_year(combined)
        log.info(
            "[RolexForums] parsed: price=%s year=%s brand=%s image=%s | %s",
            f"${price:,.0f}" if price else "—",
            year or "—",
            brand or "—",
            "yes" if image_url else "no",
            stub["title"][:80],
        )
        return {
            "title": stub["title"],
            "brand": brand,
            "model": extract_model(combined, brand),
            "reference": extract_reference(combined),
            "year": year,
            "price": price,
            "currency": "USD",
            "condition": extract_condition(combined),
            "seller": seller,
            "listing_url": url,
            "description": body[:2000],
            "image_url": image_url,
            "date_listed": stub.get("date_str"),
            "source_id": self.source_id,
        }

    def run(self, pages: int = 3, target_year: int = TARGET_YEAR,
            progress_callback=None) -> dict:
        stats = {"source": self.SOURCE_NAME, "pages": 0, "threads": 0,
                 "new": 0, "updated": 0, "priced": 0, "errors": 0,
                 "blocked": False}
        stats_lock = threading.Lock()

        def _fetch_one(stub: dict) -> tuple[dict | None, dict]:
            listing = self._parse_thread(stub["url"], stub)
            if listing:
                listing = enrich_with_price(listing, target_year)
            return listing, stub

        for page_num in range(1, pages + 1):
            url = self.FORUM_URL if page_num == 1 else f"{self.FORUM_URL}&page={page_num}"
            log.info("[RolexForums] Fetching forum index page %d…", page_num)
            resp = self._get(url)
            if not resp:
                with stats_lock:
                    stats["errors"] += 1
                    stats["blocked"] = True
                log.warning("[RolexForums] Could not fetch page %d", page_num)
                break

            stubs = self._parse_forum_page(resp.text)
            if not stubs:
                log.warning("[RolexForums] No threads found on page %d", page_num)
                with stats_lock:
                    stats["errors"] += 1
                break

            new_stubs = [
                s for s in stubs
                if is_for_sale(s["title"]) and not db.listing_url_exists(s["url"])
            ]
            log.info(
                "[RolexForums] Page %d: %d total, %d new, %d already in DB",
                page_num, len(stubs), len(new_stubs), len(stubs) - len(new_stubs),
            )
            with stats_lock:
                stats["pages"] += 1

            processed = 0
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = {pool.submit(_fetch_one, stub): stub for stub in new_stubs}
                for future in as_completed(futures):
                    stub = futures[future]
                    try:
                        listing, _ = future.result()
                    except Exception as e:
                        log.exception("[RolexForums] Worker error on %s: %s", stub.get("url"), e)
                        with stats_lock:
                            stats["errors"] += 1
                        continue

                    with stats_lock:
                        processed += 1
                        stats["threads"] += 1

                    if progress_callback:
                        progress_callback({
                            "source": self.SOURCE_NAME,
                            "current": stub["title"][:80],
                            "processed": processed,
                            "total": len(new_stubs),
                        })

                    if not listing:
                        with stats_lock:
                            stats["errors"] += 1
                        continue

                    try:
                        _, is_new = db.upsert_listing(listing)
                        with stats_lock:
                            if listing.get("price_rating") and listing["price_rating"] != "N/A":
                                stats["priced"] += 1
                            if is_new:
                                stats["new"] += 1
                            else:
                                stats["updated"] += 1
                    except Exception as e:
                        log.exception("[RolexForums] DB error for %s: %s", stub.get("url"), e)
                        with stats_lock:
                            stats["errors"] += 1

        db.mark_source_scraped(self.source_id)
        return stats


# ──────────────────────────────────────────────────────────────────────────────
#  Reddit r/Watchexchange scraper
# ──────────────────────────────────────────────────────────────────────────────

class RedditWatchexchangeScraper(BaseScraper):
    SOURCE_NAME = "Reddit r/Watchexchange"
    SUBREDDIT_JSON = "https://www.reddit.com/r/Watchexchange/new.json"
    SEARCH_JSON    = "https://www.reddit.com/r/Watchexchange/search.json"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Reddit prefers this UA format
        self.session.headers["User-Agent"] = (
            "WatchFinder/1.0 (personal project; watch listing tracker)"
        )
        self.session.headers["Accept"] = "application/json"

    def _fetch_op_comment(self, post_id: str, op_author: str) -> str:
        """Fetch the best listing-detail comment from a Reddit thread.

        r/Watchexchange sellers post as image/gallery (empty selftext) and put
        their structured listing details (price, year, condition, etc.) in a
        comment — sometimes the first OP comment, sometimes a later one.

        Strategy:
          1. Collect all OP comments; return the one containing a price pattern.
          2. If no OP comment has a price, return the longest OP comment.
          3. If OP hasn't commented yet, fall back to the first non-AutoModerator
             comment that contains a price pattern, then the longest such comment.
        """
        url = f"https://www.reddit.com/r/Watchexchange/comments/{post_id}.json"
        try:
            resp = self._get(url)
            if resp is None or resp.status_code != 200:
                return ""
            data = resp.json()
            # Response is [post_listing, comments_listing]
            if not isinstance(data, list) or len(data) < 2:
                return ""
            comments = data[1].get("data", {}).get("children", [])
            op_comments = []
            non_mod_comments = []
            for comment in comments:
                c = comment.get("data", {})
                author = c.get("author", "")
                body = c.get("body", "")
                if not body or body in ("[deleted]", "[removed]"):
                    continue
                if author.lower() == "automoderator":
                    continue
                if op_author and author.lower() == op_author.lower():
                    op_comments.append(body)
                non_mod_comments.append(body)
            # Prefer OP comment with a price; fall back to longest OP comment
            candidates = op_comments if op_comments else non_mod_comments
            with_price = [b for b in candidates if PRICE_RE.search(b)]
            if with_price:
                return max(with_price, key=len)
            if candidates:
                return max(candidates, key=len)
        except Exception as e:
            log.warning("[Reddit] Failed to fetch thread %s: %s", post_id, e)
        return ""

    def _parse_post(self, post_data: dict) -> dict | None:
        title = post_data.get("title", "")
        selftext = post_data.get("selftext") or ""  # API may return null
        url = post_data.get("url", "")
        permalink = "https://reddit.com" + post_data.get("permalink", "")
        author = post_data.get("author", "")
        created = post_data.get("created_utc")
        flair = post_data.get("link_flair_text", "") or ""
        post_id = post_data.get("id", "")

        # Only WTS and WTT (not WTB)
        if not is_for_sale(title):
            return None

        # Skip if flair is WTB
        if "WTB" in flair.upper() or "ISO" in flair.upper():
            return None

        combined = f"{title} {selftext}"

        # For image/gallery posts selftext is empty; the OP's listing details
        # (price, year, condition) live in the first non-AutoModerator comment.
        thread_body = ""
        if not selftext.strip() and post_id:
            thread_body = self._fetch_op_comment(post_id, author)
            if thread_body:
                combined = f"{combined} {thread_body}"

        brand = extract_brand(title) or extract_brand(combined)

        # Reddit posts often have an image link as the post URL
        image_url = None
        if url and re.search(r"\.(jpg|jpeg|png|webp)(\?.*)?$", url, re.I):
            image_url = url
        elif url and "imgur.com" in url and not url.endswith(".html"):
            image_url = url + ".jpg" if "." not in url.split("/")[-1] else url

        # Reddit-native images: preview.images[0].source.url (HTML-encoded)
        if not image_url:
            preview_imgs = post_data.get("preview", {}).get("images", [])
            if preview_imgs:
                src = preview_imgs[0].get("source", {}).get("url", "")
                if src:
                    image_url = src.replace("&amp;", "&")

        # Gallery posts: media_metadata contains full-size image URLs
        if not image_url and post_data.get("is_gallery"):
            metadata = post_data.get("media_metadata", {})
            for item in (post_data.get("gallery_data") or {}).get("items", []):
                mid = item.get("media_id", "")
                if mid and mid in metadata:
                    m = metadata[mid]
                    if m.get("status") == "valid":
                        src = (m.get("s") or {}).get("u", "")
                        if src:
                            image_url = src.replace("&amp;", "&")
                            break

        date_listed = None
        if created:
            try:
                date_listed = datetime.utcfromtimestamp(float(created)).isoformat()
            except Exception:
                pass

        description = selftext or thread_body
        return {
            "title": title,
            "brand": brand,
            "model": extract_model(combined, brand),
            "reference": extract_reference(combined),
            "year": extract_year(combined),
            "price": extract_price(combined),
            "currency": "USD",
            "condition": extract_condition(combined),
            "seller": author,
            "listing_url": permalink,
            "description": description[:2000],
            "image_url": image_url,
            "date_listed": date_listed,
            "source_id": self.source_id,
        }

    def run(self, pages: int = 3, target_year: int = TARGET_YEAR, progress_callback=None) -> dict:
        stats = {"source": self.SOURCE_NAME, "pages": 0, "threads": 0,
                 "new": 0, "updated": 0, "priced": 0, "errors": 0,
                 "blocked": False}

        after = None
        per_page = 100
        max_pages = pages

        for page_num in range(max_pages):
            params = {"limit": per_page, "sort": "new"}
            if after:
                params["after"] = after

            log.info("[Reddit] Fetching page %d", page_num + 1)
            try:
                url_with_params = requests.Request(
                    "GET", self.SUBREDDIT_JSON, params=params
                ).prepare().url
                resp = self._get(url_with_params)
                if resp is None:
                    stats["blocked"] = True
                    stats["errors"] += 1
                    break
                if resp.status_code == 403:
                    log.warning(
                        "[Reddit] 403 — Reddit may be blocking requests from this IP "
                        "(cloud server IPs are often blocked). Run WatchFinder locally."
                    )
                    stats["blocked"] = True
                    stats["errors"] += 1
                    break
                elif resp.status_code == 429:
                    log.warning("[Reddit] Rate limited, waiting 60 seconds…")
                    time.sleep(60)
                    continue
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.warning("[Reddit] Fetch error page %d: %s", page_num + 1, e)
                stats["errors"] += 1
                break

            children = data.get("data", {}).get("children", [])
            if not children:
                break

            stats["pages"] += 1
            after = data["data"].get("after")

            new_on_page = 0
            for child in children:
                post = child.get("data", {})
                stats["threads"] += 1
                title = post.get("title", "")
                flair = (post.get("link_flair_text") or "").upper()
                permalink = "https://reddit.com" + post.get("permalink", "")

                # Pre-filter before _parse_post() to avoid expensive
                # _fetch_op_comment() Scrape.do calls for posts we've already
                # processed or that are clearly not for-sale listings.
                if not is_for_sale(title):
                    continue
                if "WTB" in flair or "ISO" in flair:
                    continue
                if db.listing_url_exists(permalink):
                    continue

                new_on_page += 1
                try:
                    listing = self._parse_post(post)
                    if not listing:
                        continue
                    if listing.get("image_url"):
                        listing["image_url"] = self._fetch_image(listing["image_url"])
                    listing = enrich_with_price(listing, target_year)
                    if listing.get("price_rating") and listing["price_rating"] != "N/A":
                        stats["priced"] += 1
                    _, is_new = db.upsert_listing(listing)
                    if is_new:
                        stats["new"] += 1
                    else:
                        stats["updated"] += 1
                except Exception as e:
                    log.exception("[Reddit] Error: %s", e)
                    stats["errors"] += 1

            if not after:
                break
            # Stop fetching older pages once a full page has no new listings —
            # Reddit feed is newest-first so deeper pages won't have new content.
            if new_on_page == 0:
                log.info("[Reddit] No new listings on page %d, stopping early.", page_num + 1)
                break
            time.sleep(self.delay)

        db.mark_source_scraped(self.source_id)
        return stats


# ──────────────────────────────────────────────────────────────────────────────
#  Reddit price backfill
# ──────────────────────────────────────────────────────────────────────────────

def backfill_reddit_prices(target_year: int = TARGET_YEAR) -> dict:
    """Re-fetch OP comments for Reddit listings that have no price.

    Useful for listings that were scraped before the OP commented, or where
    the _fetch_op_comment call timed out during the original scrape.
    Returns stats dict with updated/skipped/errors counts.
    """
    import sqlite3
    scraper = RedditWatchexchangeScraper()
    source_id = db.get_source_id("Reddit r/Watchexchange")
    stats = {"updated": 0, "skipped": 0, "errors": 0}

    conn = sqlite3.connect(str(Path(__file__).parent / "watches.db"))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, listing_url, title, seller FROM listings "
        "WHERE source_id=? AND price IS NULL",
        (source_id,),
    ).fetchall()
    conn.close()

    log.info("[Backfill] %d Reddit listings with no price to process", len(rows))
    for row in rows:
        listing_id = row["id"]
        listing_url = row["listing_url"]
        title = row["title"]
        op_author = row["seller"] or ""
        # Extract post_id from permalink
        m = re.search(r"/comments/([a-z0-9]+)/", listing_url)
        if not m:
            stats["skipped"] += 1
            continue
        post_id = m.group(1)
        try:
            thread_body = scraper._fetch_op_comment(post_id, op_author=op_author)
            if not thread_body:
                stats["skipped"] += 1
                continue
            combined = f"{title} {thread_body}"
            price = extract_price(combined)
            if price is None:
                log.debug("[Backfill] no price in thread %s: %s", post_id, thread_body[:100])
                stats["skipped"] += 1
                continue
            brand = extract_brand(title) or extract_brand(combined)
            year = extract_year(combined)
            condition = extract_condition(combined)
            conn = sqlite3.connect(str(Path(__file__).parent / "watches.db"))
            conn.execute(
                "UPDATE listings SET price=?, brand=COALESCE(brand,?), "
                "year=COALESCE(year,?), condition=COALESCE(condition,?), "
                "description=COALESCE(NULLIF(description,''),?) WHERE id=?",
                (price, brand, year, condition, thread_body[:2000], listing_id),
            )
            conn.commit()
            conn.close()
            log.info("[Backfill] updated listing %d (%s) price=$%.0f", listing_id, post_id, price)
            stats["updated"] += 1
        except Exception as e:
            log.warning("[Backfill] error on listing %d: %s", listing_id, e)
            stats["errors"] += 1
        time.sleep(1)  # be polite to Reddit API

    log.info("[Backfill] done: %s", stats)
    return stats


# ──────────────────────────────────────────────────────────────────────────────
#  Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def run_all_scrapers(
    pages: int = 3,
    target_year: int = TARGET_YEAR,
    cookies: dict[str, str] | None = None,
    sources: list[str] | None = None,
    progress_callback=None,
) -> dict:
    """Run all configured scrapers. Returns combined stats."""
    cookies = cookies or {}
    all_stats = []

    scraper_classes = {
        "RolexForums BST": RolexForumsScraper,
        "Reddit r/Watchexchange": RedditWatchexchangeScraper,
    }

    for name, cls in scraper_classes.items():
        if sources and name not in sources:
            continue
        cookie_str = cookies.get(name, "")
        try:
            s = cls(cookie_string=cookie_str, delay=2.0)
            result = s.run(pages=pages, target_year=target_year,
                           progress_callback=progress_callback)
            all_stats.append(result)
            log.info("Scraper '%s' done: %s", name, result)
        except Exception as e:
            log.exception("Scraper '%s' failed: %s", name, e)
            all_stats.append({"source": name, "error": str(e)})

    # Aggregate
    combined = {
        "sources": all_stats,
        "new":     sum(s.get("new", 0) for s in all_stats),
        "updated": sum(s.get("updated", 0) for s in all_stats),
        "priced":  sum(s.get("priced", 0) for s in all_stats),
        "errors":  sum(s.get("errors", 0) for s in all_stats),
        "blocked": any(s.get("blocked", False) for s in all_stats),
    }
    return combined
