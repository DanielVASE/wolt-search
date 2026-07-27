"""Crawl logic: pull Wolt's Israel regions, venues per region, and per-venue menus
into the local cache. Designed to be run repeatedly and resumed — nothing here
assumes a single unattended run finishes the whole country.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from .cache import Cache
from .client import WoltClient, WoltClientError
from .retail_scraper import RetailScraper, RetailScraperError

LOG = logging.getLogger("wolt_il_search.indexer")

IL_COUNTRY_CODE = "IL"
VENUE_RADIUS_METERS = 40000  # comfortably beyond any single Wolt IL region's extent
DEFAULT_MENU_MAX_AGE_SECONDS = 7 * 24 * 3600
RETAIL_TARGET = "isr_retail_gm"  # Wolt's "Stores Around You" vertical: electronics, general merchandise, home, florists, etc.
GROCERY_TARGET = "g_retail_groceries"  # Wolt's grocery/convenience vertical (Wolt Market, AM:PM, etc.) — no "isr_" prefix, confirmed by probing the live API.

#: Pseudo-target for the restaurant vertical, which has its own endpoint
#: (`/v1/pages/restaurants`) rather than a front-page venue-list target.
RESTAURANTS_TARGET = "restaurants"


def refresh_venue_discovery(
    client: WoltClient,
    cache: Cache,
    target: str = RESTAURANTS_TARGET,
    on_region_done: Callable[[str, int], None] | None = None,
    on_progress: Callable[[dict], None] | None = None,
) -> int:
    """Sweep every cached region for venues under one Wolt discovery target.

    This generalizes what used to be three near-identical functions
    (`refresh_venues`, `refresh_retail_venues`, `refresh_grocery_venues`),
    each hardcoding one target. Any target from Wolt's own front-page
    discovery works here — including verticals and category slices this code
    has never heard of — which is what lets the admin UI build its category
    picker from the live API instead of a hand-maintained list.

    Venue -> target provenance is recorded so the UI can tell you which
    product lines a vertical actually covers.
    """
    regions = cache.list_regions()
    if not regions:
        raise RuntimeError("no regions cached — call refresh_regions() first")

    total = 0
    for index, region in enumerate(regions, start=1):
        if on_progress:
            on_progress({"ev": "tick", "i": index, "n": len(regions), "label": region["slug"], "state": "start"})
        try:
            if target == RESTAURANTS_TARGET:
                venues = client.list_venues_near(region["lat"], region["lon"], radius=VENUE_RADIUS_METERS)
            else:
                venues = client.list_venue_list(f"{target}:{region['slug']}", region["lat"], region["lon"])
        except WoltClientError as e:
            LOG.warning("target %s region %s failed: %s", target, region["slug"], e)
            if on_progress:
                on_progress(
                    {"ev": "tick", "i": index, "n": len(regions), "label": region["slug"], "ok": False, "error": str(e)}
                )
            continue

        for v in venues:
            slug = v.get("slug")
            if not slug:
                continue
            cache.upsert_venue(region["slug"], v)
            cache.record_venue_source(target, slug)
        cache.commit()
        total += len(venues)
        if on_region_done:
            on_region_done(region["slug"], len(venues))
        if on_progress:
            on_progress(
                {"ev": "tick", "i": index, "n": len(regions), "label": region["slug"], "ok": True, "found": len(venues)}
            )
    return total


def refresh_regions(client: WoltClient, cache: Cache) -> int:
    cities = client.list_cities()
    il_cities = [c for c in cities if c.get("country_code_alpha2") == IL_COUNTRY_CODE]
    for city in il_cities:
        lon, lat = city["location"]["coordinates"]
        cache.upsert_region(slug=city["slug"], name=city["name"], lat=lat, lon=lon)
    cache.commit()
    LOG.info("refreshed %d Israel regions", len(il_cities))
    return len(il_cities)


def refresh_venues(
    client: WoltClient,
    cache: Cache,
    on_region_done: Callable[[str, int], None] | None = None,
) -> int:
    """Restaurant vertical (thin wrapper kept for the existing CLI command)."""
    return refresh_venue_discovery(client, cache, RESTAURANTS_TARGET, on_region_done=on_region_done)


def refresh_retail_venues(
    client: WoltClient,
    cache: Cache,
    on_region_done: Callable[[str, int], None] | None = None,
) -> int:
    """Pull the non-restaurant retail vertical (electronics, general merchandise,
    home, florists, ...) per region. These venues never appear in
    `refresh_venues()` — Wolt organizes them under a separate front-page
    section rather than the restaurant listing endpoint.
    """
    return refresh_venue_discovery(client, cache, RETAIL_TARGET, on_region_done=on_region_done)


def refresh_grocery_venues(
    client: WoltClient,
    cache: Cache,
    on_region_done: Callable[[str, int], None] | None = None,
) -> int:
    """Pull the grocery/convenience vertical (Wolt Market, AM:PM, mini-markets,
    ...) per region. Separate front-page section from both restaurants and the
    isr_retail_gm retail vertical — none of these venues appear in either.
    """
    return refresh_venue_discovery(client, cache, GROCERY_TARGET, on_region_done=on_region_done)


def parse_menu_items(data: dict) -> list[dict]:
    category_by_item_id: dict[str, str] = {}
    for cat in data.get("categories", []):
        cat_name = cat.get("name")
        for item_id in cat.get("item_ids", []):
            category_by_item_id[item_id] = cat_name

    parsed: list[dict] = []
    for it in data.get("items", []):
        lowest = it.get("lowest_price") or {}
        parsed.append(
            {
                "id": it.get("id"),
                "name": it.get("name"),
                "description": it.get("description"),
                "category_name": category_by_item_id.get(it.get("id")),
                "price": it.get("price"),
                "original_price": it.get("original_price"),
                "lowest_price": lowest.get("price") if isinstance(lowest, dict) else None,
                "enabled": not (it.get("disabled_info") or {}).get("disabled"),
                "tags": it.get("tags") or [],
            }
        )
    return parsed


def parse_retail_items(raw_items: list[dict]) -> list[dict]:
    parsed: list[dict] = []
    for it in raw_items:
        lowest = it.get("lowest_price") or {}
        parsed.append(
            {
                "id": it.get("id"),
                "name": it.get("name"),
                "description": it.get("description"),
                "category_name": it.get("_category_name"),
                "price": it.get("price"),
                "original_price": it.get("original_price"),
                "lowest_price": lowest.get("price") if isinstance(lowest, dict) else None,
                "enabled": not (it.get("disabled_info") or {}).get("disabled"),
                "tags": it.get("tags") or [],
            }
        )
    return parsed


def _handle_fetch_failure(
    cache: Cache,
    slug: str,
    error: Exception,
    on_venue_done: Callable[[str, int | None], None] | None,
    on_progress: Callable[[dict], None] | None,
    index: int,
    total: int,
) -> None:
    """Record a failed venue fetch honestly.

    The old behaviour was `cache.mark_menu_fetched(slug)` for *every* failure,
    with the comment "avoid hot-looping on a permanently broken slug". That
    conflated two very different cases: a 404 (genuinely broken) and a 429 or
    timeout (we were simply going too fast). Because the retry policy in
    backoff.py has already exhausted its attempts by the time we get here, a
    transient failure used to stamp the venue as freshly-fetched-with-no-items
    and hide it from crawls for the entire max-age window.
    """
    permanent = bool(getattr(error, "permanent", False))
    fails = cache.record_fetch_failure(slug, str(error), permanent=permanent)
    cache.commit()
    if on_venue_done:
        on_venue_done(slug, None)
    if on_progress:
        on_progress(
            {
                "ev": "tick",
                "i": index,
                "n": total,
                "label": slug,
                "ok": False,
                "permanent": permanent,
                "fails": fails,
                "error": str(error)[:200],
            }
        )


def refresh_scraped_items(
    scraper: RetailScraper,
    cache: Cache,
    limit: int = 50,
    max_age_seconds: float = DEFAULT_MENU_MAX_AGE_SECONDS,
    product_lines: tuple[str, ...] | None = None,
    on_venue_done: Callable[[str, int | None], None] | None = None,
    on_progress: Callable[[dict], None] | None = None,
) -> tuple[int, int]:
    """Fetch item catalogs via the HTML category-page scraper (see
    retail_scraper.py) for venues whose JSON assortment API always returns
    zero items — electronics and grocery, both loading_strategy="partial".
    Pass product_lines=("electronics",) or ("grocery",) to run just one.

    `limit <= 0` means "every remaining candidate" (see Cache._sql_limit),
    which is how the admin UI's default "crawl the whole category" works.
    """
    candidates = cache.retail_catalog_venues_needing_menu(
        max_age_seconds=max_age_seconds, limit=limit, product_lines=product_lines
    )
    total = len(candidates)
    if on_progress:
        on_progress({"ev": "total", "n": total})
    attempted = 0
    succeeded = 0
    for venue in candidates:
        attempted += 1
        slug = venue["slug"]
        if on_progress:
            on_progress({"ev": "tick", "i": attempted, "n": total, "label": slug, "state": "start"})
        try:
            raw_items = scraper.crawl_venue_items(venue["region_slug"], slug)
        except RetailScraperError as e:
            LOG.warning("retail item fetch failed for %s: %s", slug, e)
            _handle_fetch_failure(cache, slug, e, on_venue_done, on_progress, attempted, total)
            continue

        items = parse_retail_items(raw_items)
        cache.replace_menu_items(slug, items)
        cache.mark_menu_fetched(slug)
        cache.commit()
        succeeded += 1
        if on_venue_done:
            on_venue_done(slug, len(items))
        if on_progress:
            on_progress({"ev": "tick", "i": attempted, "n": total, "label": slug, "ok": True, "items": len(items)})
    return attempted, succeeded


def refresh_empty_catalog_venues(
    scraper: RetailScraper,
    cache: Cache,
    product_lines: tuple[str, ...] = ("general_merchandise", "home_and_diy"),
    limit: int = 50,
    max_age_seconds: float = DEFAULT_MENU_MAX_AGE_SECONDS,
    on_venue_done: Callable[[str, int | None], None] | None = None,
    on_progress: Callable[[dict], None] | None = None,
) -> tuple[int, int]:
    """Follow-up pass for non-electronics retail venues that came back with
    zero items via the plain JSON path — those use loading_strategy="partial"
    too and need the same HTML scraper electronics venues do.

    The default product_lines here are only a fallback for the legacy CLI
    command; the crawl planner passes whatever the operator selected, so this
    fallback now applies to florist/pharmacy/pet_supply/... too instead of
    just the two lines that used to be hardcoded in the admin UI's argv.
    """
    candidates = cache.venues_with_empty_catalog(product_lines, limit=limit, max_age_seconds=max_age_seconds)
    total = len(candidates)
    if on_progress:
        on_progress({"ev": "total", "n": total})
    attempted = 0
    succeeded = 0
    for venue in candidates:
        attempted += 1
        slug = venue["slug"]
        if on_progress:
            on_progress({"ev": "tick", "i": attempted, "n": total, "label": slug, "state": "start"})
        cache.mark_menu_stale(slug)
        # Record the scrape attempt up front: whether it yields items or not,
        # this venue shouldn't be re-scraped by the next run until max-age
        # passes (an HTML scrape is many requests per venue).
        cache.mark_scrape_attempted(slug)
        cache.commit()  # release this write before the slow network scrape below —
        # otherwise the uncommitted transaction blocks other writers for the
        # scrape's whole duration (can be a minute+ for many-category venues)
        try:
            raw_items = scraper.crawl_venue_items(venue["region_slug"], slug)
        except RetailScraperError as e:
            LOG.warning("empty-catalog rescrape failed for %s: %s", slug, e)
            _handle_fetch_failure(cache, slug, e, on_venue_done, on_progress, attempted, total)
            continue

        items = parse_retail_items(raw_items)
        cache.replace_menu_items(slug, items)
        cache.mark_menu_fetched(slug)
        cache.commit()
        succeeded += 1
        if on_venue_done:
            on_venue_done(slug, len(items))
        if on_progress:
            on_progress({"ev": "tick", "i": attempted, "n": total, "label": slug, "ok": True, "items": len(items)})
    return attempted, succeeded


def refresh_menus(
    client: WoltClient,
    cache: Cache,
    limit: int = 200,
    max_age_seconds: float = DEFAULT_MENU_MAX_AGE_SECONDS,
    product_lines: tuple[str, ...] | None = None,
    on_venue_done: Callable[[str, int | None], None] | None = None,
    on_progress: Callable[[dict], None] | None = None,
) -> tuple[int, int]:
    """Fetch menus for up to `limit` venues whose menu is missing or stale.

    `limit <= 0` means every remaining candidate.

    Returns (attempted, succeeded).
    """
    candidates = cache.venues_needing_menu(max_age_seconds=max_age_seconds, limit=limit, product_lines=product_lines)
    total = len(candidates)
    if on_progress:
        on_progress({"ev": "total", "n": total})
    attempted = 0
    succeeded = 0
    for venue in candidates:
        attempted += 1
        slug = venue["slug"]
        if on_progress:
            on_progress({"ev": "tick", "i": attempted, "n": total, "label": slug, "state": "start"})
        try:
            data = client.get_assortment(slug)
        except WoltClientError as e:
            LOG.warning("menu fetch failed for %s: %s", slug, e)
            _handle_fetch_failure(cache, slug, e, on_venue_done, on_progress, attempted, total)
            continue

        items = parse_menu_items(data)
        cache.replace_menu_items(slug, items)
        cache.mark_menu_fetched(slug)
        cache.commit()
        succeeded += 1
        if on_venue_done:
            on_venue_done(slug, len(items))
        if on_progress:
            on_progress({"ev": "tick", "i": attempted, "n": total, "label": slug, "ok": True, "items": len(items)})
    return attempted, succeeded
