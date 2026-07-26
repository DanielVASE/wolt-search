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
    regions = cache.list_regions()
    if not regions:
        raise RuntimeError("no regions cached — call refresh_regions() first")

    total = 0
    for region in regions:
        try:
            venues = client.list_venues_near(region["lat"], region["lon"], radius=VENUE_RADIUS_METERS)
        except WoltClientError as e:
            LOG.warning("region %s failed: %s", region["slug"], e)
            continue
        for v in venues:
            slug = v.get("slug")
            if not slug:
                continue
            cache.upsert_venue(region["slug"], v)
        cache.commit()
        total += len(venues)
        if on_region_done:
            on_region_done(region["slug"], len(venues))
    return total


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
    regions = cache.list_regions()
    if not regions:
        raise RuntimeError("no regions cached — call refresh_regions() first")

    total = 0
    for region in regions:
        target = f"{RETAIL_TARGET}:{region['slug']}"
        try:
            venues = client.list_venue_list(target, region["lat"], region["lon"])
        except WoltClientError as e:
            LOG.warning("retail region %s failed: %s", region["slug"], e)
            continue
        for v in venues:
            slug = v.get("slug")
            if not slug:
                continue
            cache.upsert_venue(region["slug"], v)
        cache.commit()
        total += len(venues)
        if on_region_done:
            on_region_done(region["slug"], len(venues))
    return total


def refresh_grocery_venues(
    client: WoltClient,
    cache: Cache,
    on_region_done: Callable[[str, int], None] | None = None,
) -> int:
    """Pull the grocery/convenience vertical (Wolt Market, AM:PM, mini-markets,
    ...) per region. Separate front-page section from both restaurants and the
    isr_retail_gm retail vertical — none of these venues appear in either.
    """
    regions = cache.list_regions()
    if not regions:
        raise RuntimeError("no regions cached — call refresh_regions() first")

    total = 0
    for region in regions:
        target = f"{GROCERY_TARGET}:{region['slug']}"
        try:
            venues = client.list_venue_list(target, region["lat"], region["lon"])
        except WoltClientError as e:
            LOG.warning("grocery region %s failed: %s", region["slug"], e)
            continue
        for v in venues:
            slug = v.get("slug")
            if not slug:
                continue
            cache.upsert_venue(region["slug"], v)
        cache.commit()
        total += len(venues)
        if on_region_done:
            on_region_done(region["slug"], len(venues))
    return total


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


def refresh_scraped_items(
    scraper: RetailScraper,
    cache: Cache,
    limit: int = 50,
    max_age_seconds: float = DEFAULT_MENU_MAX_AGE_SECONDS,
    product_lines: tuple[str, ...] | None = None,
    on_venue_done: Callable[[str, int | None], None] | None = None,
) -> tuple[int, int]:
    """Fetch item catalogs via the HTML category-page scraper (see
    retail_scraper.py) for venues whose JSON assortment API always returns
    zero items — electronics and grocery, both loading_strategy="partial".
    Pass product_lines=("electronics",) or ("grocery",) to run just one.
    """
    candidates = cache.retail_catalog_venues_needing_menu(
        max_age_seconds=max_age_seconds, limit=limit, product_lines=product_lines
    )
    attempted = 0
    succeeded = 0
    for venue in candidates:
        attempted += 1
        slug = venue["slug"]
        try:
            raw_items = scraper.crawl_venue_items(venue["region_slug"], slug)
        except RetailScraperError as e:
            LOG.warning("retail item fetch failed for %s: %s", slug, e)
            cache.mark_menu_fetched(slug)
            cache.commit()
            if on_venue_done:
                on_venue_done(slug, None)
            continue

        items = parse_retail_items(raw_items)
        cache.replace_menu_items(slug, items)
        cache.mark_menu_fetched(slug)
        cache.commit()
        succeeded += 1
        if on_venue_done:
            on_venue_done(slug, len(items))
    return attempted, succeeded


def refresh_empty_catalog_venues(
    scraper: RetailScraper,
    cache: Cache,
    product_lines: tuple[str, ...] = ("general_merchandise", "home_and_diy"),
    limit: int = 50,
    max_age_seconds: float = DEFAULT_MENU_MAX_AGE_SECONDS,
    on_venue_done: Callable[[str, int | None], None] | None = None,
) -> tuple[int, int]:
    """Follow-up pass for non-electronics retail venues that came back with
    zero items via the plain JSON path — those use loading_strategy="partial"
    too and need the same HTML scraper electronics venues do.
    """
    candidates = cache.venues_with_empty_catalog(product_lines, limit=limit, max_age_seconds=max_age_seconds)
    attempted = 0
    succeeded = 0
    for venue in candidates:
        attempted += 1
        slug = venue["slug"]
        cache.mark_menu_stale(slug)
        cache.commit()  # release this write before the slow network scrape below —
        # otherwise the uncommitted transaction blocks other writers for the
        # scrape's whole duration (can be a minute+ for many-category venues)
        try:
            raw_items = scraper.crawl_venue_items(venue["region_slug"], slug)
        except RetailScraperError as e:
            LOG.warning("empty-catalog rescrape failed for %s: %s", slug, e)
            cache.mark_menu_fetched(slug)
            cache.commit()
            if on_venue_done:
                on_venue_done(slug, None)
            continue

        items = parse_retail_items(raw_items)
        cache.replace_menu_items(slug, items)
        cache.mark_menu_fetched(slug)
        cache.commit()
        succeeded += 1
        if on_venue_done:
            on_venue_done(slug, len(items))
    return attempted, succeeded


def refresh_menus(
    client: WoltClient,
    cache: Cache,
    limit: int = 200,
    max_age_seconds: float = DEFAULT_MENU_MAX_AGE_SECONDS,
    product_lines: tuple[str, ...] | None = None,
    on_venue_done: Callable[[str, int | None], None] | None = None,
) -> tuple[int, int]:
    """Fetch menus for up to `limit` venues whose menu is missing or stale.

    Returns (attempted, succeeded).
    """
    candidates = cache.venues_needing_menu(max_age_seconds=max_age_seconds, limit=limit, product_lines=product_lines)
    attempted = 0
    succeeded = 0
    for venue in candidates:
        attempted += 1
        slug = venue["slug"]
        try:
            data = client.get_assortment(slug)
        except WoltClientError as e:
            LOG.warning("menu fetch failed for %s: %s", slug, e)
            cache.mark_menu_fetched(slug)  # avoid hot-looping on a permanently broken slug
            cache.commit()
            if on_venue_done:
                on_venue_done(slug, None)
            continue

        items = parse_menu_items(data)
        cache.replace_menu_items(slug, items)
        cache.mark_menu_fetched(slug)
        cache.commit()
        succeeded += 1
        if on_venue_done:
            on_venue_done(slug, len(items))
    return attempted, succeeded
