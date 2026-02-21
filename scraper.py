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
import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

import database as db
import price_checker

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
    r"\$\s*([\d,]+(?:\.\d{1,2})?)"          # $1,234.00
    r"|([\d,]+(?:\.\d{1,2})?)\s*USD"        # 1234 USD
    r"|([\d,.]+)\s*k\b"                      # 8.5k / 8k
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
        dollar, usd, k = m.group(1), m.group(2), m.group(3)
        raw = (dollar or usd or k or "").replace(",", "")
        try:
            val = float(raw)
            if k:
                val *= 1000
            if 200 <= val <= 500_000:
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
    lower = text.lower()
    for key, val in sorted(BRAND_ALIASES.items(), key=lambda x: -len(x[0])):
        if key in lower:
            return val
    return None


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
            log.warning("HTTP error fetching %s: %s", url, e)
        except requests.RequestException as e:
            log.warning("Request error fetching %s: %s", url, e)
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
        post = (
            soup.find("div", class_=re.compile(r"postbody|post-content|content"))
            or soup.find("blockquote", class_=re.compile(r"postcontent|restore"))
        )
        body = post.get_text(" ", strip=True) if post else ""
        combined = f"{stub['title']} {body}"

        image_url = None
        if post:
            img = post.find("img", src=re.compile(r"\.(jpg|jpeg|png|webp)", re.I))
            if img:
                image_url = img.get("src") or img.get("data-src")

        seller = stub.get("seller")
        if not seller:
            at = soup.find(class_=re.compile(r"username|author"))
            if at:
                seller = at.get_text(strip=True)

        brand = extract_brand(combined)
        return {
            "title": stub["title"],
            "brand": brand,
            "model": extract_model(combined, brand),
            "reference": extract_reference(combined),
            "year": extract_year(combined),
            "price": extract_price(combined),
            "currency": "USD",
            "condition": extract_condition(combined),
            "seller": seller,
            "listing_url": url,
            "description": body[:2000],
            "image_url": image_url,
            "date_listed": stub.get("date_str"),
            "source_id": self.source_id,
        }

    def run(self, pages: int = 3, target_year: int = TARGET_YEAR) -> dict:
        stats = {"source": self.SOURCE_NAME, "pages": 0, "threads": 0,
                 "new": 0, "updated": 0, "priced": 0, "errors": 0,
                 "blocked": False}

        for page in range(1, pages + 1):
            url = self.FORUM_URL if page == 1 else f"{self.FORUM_URL}&page={page}"
            log.info("[RolexForums] Scraping page %d: %s", page, url)
            resp = self._get(url)
            if not resp:
                stats["errors"] += 1
                stats["blocked"] = True
                log.warning(
                    "[RolexForums] Could not fetch page %d — may be blocked by Cloudflare. "
                    "Run WatchFinder locally with residential internet and provide session cookies.",
                    page,
                )
                break

            stubs = self._parse_forum_page(resp.text)
            if not stubs:
                log.warning("[RolexForums] No threads found on page %d", page)
                stats["errors"] += 1
                break

            stats["pages"] += 1
            for stub in stubs:
                if not is_for_sale(stub["title"]):
                    continue
                stats["threads"] += 1
                time.sleep(self.delay)
                try:
                    listing = self._parse_thread(stub["url"], stub)
                    if not listing:
                        stats["errors"] += 1
                        continue
                    listing = enrich_with_price(listing, target_year)
                    if listing.get("price_rating") and listing["price_rating"] != "N/A":
                        stats["priced"] += 1
                    _, is_new = db.upsert_listing(listing)
                    if is_new:
                        stats["new"] += 1
                    else:
                        stats["updated"] += 1
                except Exception as e:
                    log.exception("[RolexForums] Error processing %s: %s", stub.get("url"), e)
                    stats["errors"] += 1

            time.sleep(self.delay)

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

    def _parse_post(self, post_data: dict) -> dict | None:
        title = post_data.get("title", "")
        selftext = post_data.get("selftext", "")
        url = post_data.get("url", "")
        permalink = "https://reddit.com" + post_data.get("permalink", "")
        author = post_data.get("author", "")
        created = post_data.get("created_utc")
        flair = post_data.get("link_flair_text", "") or ""

        # Only WTS and WTT (not WTB)
        if not is_for_sale(title):
            return None

        # Skip if flair is WTB
        if "WTB" in flair.upper() or "ISO" in flair.upper():
            return None

        combined = f"{title} {selftext}"
        brand = extract_brand(combined)

        # Reddit posts often have an image link as the post URL
        image_url = None
        if url and re.search(r"\.(jpg|jpeg|png|webp)(\?.*)?$", url, re.I):
            image_url = url
        elif url and "imgur.com" in url and not url.endswith(".html"):
            image_url = url + ".jpg" if "." not in url.split("/")[-1] else url

        date_listed = None
        if created:
            try:
                date_listed = datetime.utcfromtimestamp(float(created)).isoformat()
            except Exception:
                pass

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
            "description": selftext[:2000],
            "image_url": image_url,
            "date_listed": date_listed,
            "source_id": self.source_id,
        }

    def run(self, pages: int = 3, target_year: int = TARGET_YEAR) -> dict:
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
                resp = self.session.get(self.SUBREDDIT_JSON, params=params, timeout=20)
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

            for child in children:
                post = child.get("data", {})
                stats["threads"] += 1
                try:
                    listing = self._parse_post(post)
                    if not listing:
                        continue
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
            time.sleep(self.delay)

        db.mark_source_scraped(self.source_id)
        return stats


# ──────────────────────────────────────────────────────────────────────────────
#  Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def run_all_scrapers(
    pages: int = 3,
    target_year: int = TARGET_YEAR,
    cookies: dict[str, str] | None = None,
    sources: list[str] | None = None,
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
            result = s.run(pages=pages, target_year=target_year)
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
