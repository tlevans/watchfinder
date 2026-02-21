"""
WatchCharts price checker.

Fetches market price data from marketplace.watchcharts.com and rates a
listing price as: Great / Good / Fair / High.
"""
import re
import time
import logging
import requests
from urllib.parse import quote_plus

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# WatchCharts internal API (discovered from network inspection)
WATCHCHARTS_SEARCH_URL = "https://watchcharts.com/api/watches/search/"
WATCHCHARTS_CHART_URL = "https://watchcharts.com/watches/{slug}"
WATCHCHARTS_MARKETPLACE_URL = "https://marketplace.watchcharts.com/listings"

# Deal rating thresholds (% of market price)
GREAT_THRESHOLD = 0.90   # <= 90%  of market → Great Deal
GOOD_THRESHOLD  = 1.00   # <= 100% of market → Good Deal
FAIR_THRESHOLD  = 1.10   # <= 110% of market → Fair Price
#                          > 110% of market → High Price


def rate_price(listing_price: float, market_price: float) -> tuple[str, float]:
    """Return (rating_label, pct_of_market) for listing_price vs market_price."""
    if market_price <= 0:
        return "N/A", 0.0
    ratio = listing_price / market_price
    if ratio <= GREAT_THRESHOLD:
        return "Great", ratio
    elif ratio <= GOOD_THRESHOLD:
        return "Good", ratio
    elif ratio <= FAIR_THRESHOLD:
        return "Fair", ratio
    else:
        return "High", ratio


def _search_watchcharts(query: str) -> list[dict]:
    """Search WatchCharts for watches matching query. Returns list of results."""
    try:
        # Try the search API endpoint
        resp = SESSION.get(
            WATCHCHARTS_SEARCH_URL,
            params={"q": query, "limit": 5},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "results" in data:
                return data["results"]
    except Exception as e:
        log.debug("WatchCharts API search failed: %s", e)

    # Fallback: scrape the search page
    try:
        resp = SESSION.get(
            "https://watchcharts.com/watches",
            params={"q": query},
            timeout=10,
        )
        if resp.status_code == 200:
            return _parse_search_html(resp.text)
    except Exception as e:
        log.debug("WatchCharts HTML search failed: %s", e)

    return []


def _parse_search_html(html: str) -> list[dict]:
    """Parse watch search results from WatchCharts HTML page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    results = []

    # WatchCharts uses next.js — look for __NEXT_DATA__ JSON
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        import json
        try:
            page_data = json.loads(script.string)
            # Navigate the Next.js props tree
            props = page_data.get("props", {}).get("pageProps", {})
            watches = props.get("watches") or props.get("results") or []
            for w in watches[:5]:
                results.append({
                    "id": w.get("id"),
                    "slug": w.get("slug"),
                    "brand": w.get("brand", {}).get("name") if isinstance(w.get("brand"), dict) else w.get("brand"),
                    "model": w.get("model"),
                    "reference": w.get("reference"),
                    "market_price": w.get("market_price") or w.get("price"),
                })
        except Exception as e:
            log.debug("Failed to parse __NEXT_DATA__: %s", e)

    return results


def _get_market_price_from_chart(slug: str) -> float | None:
    """Fetch market price from a watch's chart page."""
    try:
        resp = SESSION.get(
            f"https://watchcharts.com/watches/{slug}",
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        from bs4 import BeautifulSoup
        import json
        soup = BeautifulSoup(resp.text, "lxml")
        script = soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            data = json.loads(script.string)
            props = data.get("props", {}).get("pageProps", {})
            watch = props.get("watch") or props.get("watchData") or {}

            # Try common price field names
            for field in ["market_price", "price", "current_price", "avg_price",
                          "median_price", "priceData"]:
                val = watch.get(field)
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
                if isinstance(val, dict):
                    for sub in ["value", "amount", "price", "median"]:
                        sv = val.get(sub)
                        if isinstance(sv, (int, float)) and sv > 0:
                            return float(sv)

    except Exception as e:
        log.debug("Failed to get market price from chart page: %s", e)

    return None


def _get_market_price_from_marketplace(brand: str, reference: str) -> float | None:
    """
    Fetch median listing price from WatchCharts marketplace as a fallback market price.
    """
    try:
        params = {"brand": brand}
        if reference:
            params["ref"] = reference
        resp = SESSION.get(WATCHCHARTS_MARKETPLACE_URL, params=params, timeout=12)
        if resp.status_code != 200:
            return None

        from bs4 import BeautifulSoup
        import json
        soup = BeautifulSoup(resp.text, "lxml")
        script = soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            data = json.loads(script.string)
            props = data.get("props", {}).get("pageProps", {})

            # Try to find listing prices
            listings = props.get("listings") or props.get("data") or []
            prices = []
            for item in listings:
                p = item.get("price") or item.get("amount")
                if isinstance(p, (int, float)) and p > 0:
                    prices.append(float(p))

            if prices:
                prices.sort()
                # Use median
                mid = len(prices) // 2
                return prices[mid]

    except Exception as e:
        log.debug("Marketplace fallback failed: %s", e)

    return None


def check_price(
    brand: str,
    model: str,
    reference: str,
    listing_price: float,
    year: int | None = None,
) -> dict:
    """
    Main entry point. Returns dict with:
      rating       : 'Great' | 'Good' | 'Fair' | 'High' | 'N/A'
      market_price : float | None
      pct_of_market: float | None
      watchcharts_url: str | None
      source       : str (how price was obtained)
    """
    result = {
        "rating": "N/A",
        "market_price": None,
        "pct_of_market": None,
        "watchcharts_url": None,
        "source": "none",
    }

    if not listing_price or listing_price <= 0:
        return result

    market_price = None
    wc_url = None
    source = "none"

    # Build search query
    query_parts = [brand]
    if reference:
        query_parts.append(reference)
    elif model:
        query_parts.append(model)
    query = " ".join(query_parts)

    log.info("Checking WatchCharts for: %s (listing $%.0f)", query, listing_price)

    # 1. Search WatchCharts
    results = _search_watchcharts(query)
    if results:
        best = results[0]
        slug = best.get("slug") or str(best.get("id", ""))
        raw_price = best.get("market_price") or best.get("price")

        if raw_price and float(raw_price) > 0:
            market_price = float(raw_price)
            source = "search_result"
        elif slug:
            time.sleep(0.5)
            market_price = _get_market_price_from_chart(slug)
            if market_price:
                source = "chart_page"

        if slug:
            wc_url = f"https://watchcharts.com/watches/{slug}"

    # 2. Fallback: marketplace median
    if not market_price:
        time.sleep(0.5)
        market_price = _get_market_price_from_marketplace(brand, reference or model)
        if market_price:
            source = "marketplace_median"
            wc_url = (
                WATCHCHARTS_MARKETPLACE_URL
                + f"?brand={quote_plus(brand)}&ref={quote_plus(reference or model)}"
            )

    if not market_price:
        log.warning("Could not obtain market price for %s", query)
        return result

    rating, ratio = rate_price(listing_price, market_price)
    result.update({
        "rating": rating,
        "market_price": round(market_price, 2),
        "pct_of_market": round(ratio * 100, 1),
        "watchcharts_url": wc_url,
        "source": source,
    })
    log.info(
        "%s %s: market=$%.0f listing=$%.0f → %s (%.1f%%)",
        brand, reference or model, market_price, listing_price, rating, ratio * 100,
    )
    return result
