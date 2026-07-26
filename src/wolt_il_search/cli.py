"""Command-line entry point for building/refreshing the local Wolt Israel cache.

Menu indexing means one HTTP request per venue (thousands, nationwide), so
this is deliberately a batched, resumable, rate-limited crawl you run
yourself (once, then periodically) rather than something triggered inline by
an MCP tool call.
"""

from __future__ import annotations

import argparse
import logging
import sys

from .cache import Cache
from .client import WoltClient
from .indexer import (
    DEFAULT_MENU_MAX_AGE_SECONDS,
    refresh_empty_catalog_venues,
    refresh_grocery_venues,
    refresh_menus,
    refresh_regions,
    refresh_retail_venues,
    refresh_scraped_items,
    refresh_venues,
)
from .retail_scraper import RetailScraper
from .search import search as run_search

LOG = logging.getLogger("wolt_il_search.cli")


def cmd_regions(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    client = WoltClient(delay_seconds=args.delay)
    n = refresh_regions(client, cache)
    print(f"refreshed {n} Israel regions")


def cmd_venues(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    client = WoltClient(delay_seconds=args.delay)

    def on_done(slug: str, count: int) -> None:
        print(f"  {slug}: {count} venues")

    total = refresh_venues(client, cache, on_region_done=on_done)
    cache.rebuild_search_index()
    print(f"refreshed {total} venue entries across all regions")


def cmd_menus(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    client = WoltClient(delay_seconds=args.delay)

    def on_done(slug: str, item_count: int | None) -> None:
        if item_count is None:
            print(f"  {slug}: FAILED")
        else:
            print(f"  {slug}: {item_count} items")

    product_lines = tuple(args.product_lines.split(",")) if args.product_lines else None
    attempted, succeeded = refresh_menus(
        client,
        cache,
        limit=args.limit,
        max_age_seconds=args.max_age_hours * 3600,
        product_lines=product_lines,
        on_venue_done=on_done,
    )
    cache.rebuild_search_index()
    print(f"attempted {attempted} venue menus, {succeeded} succeeded")
    counts = cache.counts()
    print(f"cache now: {counts}")


def cmd_full(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    client = WoltClient(delay_seconds=args.delay)

    n_regions = refresh_regions(client, cache)
    print(f"refreshed {n_regions} regions")

    total_venues = refresh_venues(client, cache)
    print(f"refreshed {total_venues} venue entries")

    attempted, succeeded = refresh_menus(
        client, cache, limit=args.menu_limit, max_age_seconds=DEFAULT_MENU_MAX_AGE_SECONDS
    )
    cache.rebuild_search_index()
    print(f"fetched menus for {succeeded}/{attempted} venues this run")
    print(f"cache now: {cache.counts()}")


def cmd_retail_venues(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    client = WoltClient(delay_seconds=args.delay)

    def on_done(slug: str, count: int) -> None:
        print(f"  {slug}: {count} retail venues")

    total = refresh_retail_venues(client, cache, on_region_done=on_done)
    cache.rebuild_search_index()
    print(f"refreshed {total} retail venue entries across all regions")


def cmd_electronics_items(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    scraper = RetailScraper(delay_seconds=args.delay)

    def on_done(slug: str, item_count: int | None) -> None:
        if item_count is None:
            print(f"  {slug}: FAILED")
        else:
            print(f"  {slug}: {item_count} items")

    attempted, succeeded = refresh_scraped_items(
        scraper,
        cache,
        limit=args.limit,
        max_age_seconds=args.max_age_hours * 3600,
        product_lines=("electronics",),
        on_venue_done=on_done,
    )
    cache.rebuild_search_index()
    print(f"attempted {attempted} electronics venues, {succeeded} succeeded")
    print(f"cache now: {cache.counts()}")


def cmd_grocery_venues(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    client = WoltClient(delay_seconds=args.delay)

    def on_done(slug: str, count: int) -> None:
        print(f"  {slug}: {count} grocery venues")

    total = refresh_grocery_venues(client, cache, on_region_done=on_done)
    cache.rebuild_search_index()
    print(f"refreshed {total} grocery venue entries across all regions")


def cmd_grocery_items(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    scraper = RetailScraper(delay_seconds=args.delay)

    def on_done(slug: str, item_count: int | None) -> None:
        if item_count is None:
            print(f"  {slug}: FAILED")
        else:
            print(f"  {slug}: {item_count} items")

    attempted, succeeded = refresh_scraped_items(
        scraper,
        cache,
        limit=args.limit,
        max_age_seconds=args.max_age_hours * 3600,
        product_lines=("grocery",),
        on_venue_done=on_done,
    )
    cache.rebuild_search_index()
    print(f"attempted {attempted} grocery venues, {succeeded} succeeded")
    print(f"cache now: {cache.counts()}")


def cmd_retail_rescrape(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    scraper = RetailScraper(delay_seconds=args.delay)
    product_lines = tuple(args.product_lines.split(","))

    def on_done(slug: str, item_count: int | None) -> None:
        if item_count is None:
            print(f"  {slug}: FAILED")
        else:
            print(f"  {slug}: {item_count} items")

    attempted, succeeded = refresh_empty_catalog_venues(
        scraper,
        cache,
        product_lines=product_lines,
        limit=args.limit,
        max_age_seconds=args.max_age_hours * 3600,
        on_venue_done=on_done,
    )
    cache.rebuild_search_index()
    print(f"attempted {attempted} empty-catalog venues, {succeeded} succeeded")
    print(f"cache now: {cache.counts()}")


def cmd_reindex(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    cache.rebuild_search_index()
    print("search index rebuilt")


def cmd_status(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    print(cache.counts())


def cmd_search(args: argparse.Namespace) -> None:
    cache = Cache(args.db)
    results = run_search(
        cache,
        args.query,
        region=args.region,
        only_open=args.open,
        min_rating=args.min_rating,
        product_line=args.product_line,
        max_results=args.max_results,
    )
    for r in results:
        status = "open" if r.online else "closed"
        print(f"[{r.score:5.1f}] {r.name}  ({r.region_slug}, {status}, rating={r.rating})  -> {r.address}")
        print(f"          {r.wolt_url}")
        for it in r.matched_items:
            price = f"{it.price / 100:.2f}" if it.price is not None else "?"
            print(f"          - {it.name} ({price}) [{it.category_name}]  {it.wolt_url}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wolt-il-refresh")
    p.add_argument("--db", default=None, help="Path to the SQLite cache (default: ~/.wolt-il-search/cache.db)")
    p.add_argument("--delay", type=float, default=1.2, help="Seconds between Wolt requests (rate limiting)")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("regions", help="Refresh the list of Israel regions from Wolt").set_defaults(func=cmd_regions)

    sub.add_parser("venues", help="Refresh venues for every cached region").set_defaults(func=cmd_venues)

    p_menus = sub.add_parser("menus", help="Fetch menus for a batch of missing/stale venues")
    p_menus.add_argument("--limit", type=int, default=200, help="Max venues to fetch this run")
    p_menus.add_argument("--max-age-hours", type=float, default=DEFAULT_MENU_MAX_AGE_SECONDS / 3600)
    p_menus.add_argument(
        "--product-lines", default=None, help="Comma-separated product_line filter, e.g. general_merchandise,home_and_diy"
    )
    p_menus.set_defaults(func=cmd_menus)

    p_full = sub.add_parser("full", help="Regions + venues + one batch of menus (good for a cron/automation)")
    p_full.add_argument("--menu-limit", type=int, default=200)
    p_full.set_defaults(func=cmd_full)

    sub.add_parser(
        "retail-venues", help="Refresh the retail vertical (electronics, general merchandise, ...) for every region"
    ).set_defaults(func=cmd_retail_venues)

    p_electronics = sub.add_parser(
        "electronics-items",
        help="Fetch item catalogs for electronics/computer retail venues (HTML scrape, slower than 'menus')",
    )
    p_electronics.add_argument("--limit", type=int, default=50, help="Max venues to fetch this run")
    p_electronics.add_argument("--max-age-hours", type=float, default=DEFAULT_MENU_MAX_AGE_SECONDS / 3600)
    p_electronics.set_defaults(func=cmd_electronics_items)

    p_rescrape = sub.add_parser(
        "retail-rescrape",
        help="HTML-scrape follow-up for non-electronics retail venues that came back empty via the plain JSON path",
    )
    p_rescrape.add_argument("--limit", type=int, default=50)
    p_rescrape.add_argument("--max-age-hours", type=float, default=DEFAULT_MENU_MAX_AGE_SECONDS / 3600)
    p_rescrape.add_argument("--product-lines", default="general_merchandise,home_and_diy")
    p_rescrape.set_defaults(func=cmd_retail_rescrape)

    sub.add_parser(
        "grocery-venues", help="Refresh the grocery/convenience vertical (Wolt Market, AM:PM, ...) for every region"
    ).set_defaults(func=cmd_grocery_venues)

    p_grocery = sub.add_parser(
        "grocery-items",
        help="Fetch item catalogs for grocery/convenience venues (HTML scrape, same mechanism as electronics-items)",
    )
    p_grocery.add_argument("--limit", type=int, default=50, help="Max venues to fetch this run")
    p_grocery.add_argument("--max-age-hours", type=float, default=DEFAULT_MENU_MAX_AGE_SECONDS / 3600)
    p_grocery.set_defaults(func=cmd_grocery_items)

    sub.add_parser("reindex", help="Rebuild the FTS5 search index from current cache contents").set_defaults(
        func=cmd_reindex
    )

    sub.add_parser("status", help="Show cache counts").set_defaults(func=cmd_status)

    p_search = sub.add_parser("search", help="Run a search against the local cache (for testing)")
    p_search.add_argument("query")
    p_search.add_argument("--region", default=None)
    p_search.add_argument("--open", action="store_true")
    p_search.add_argument("--min-rating", type=float, default=None)
    p_search.add_argument("--product-line", default=None, help="e.g. restaurant, electronics, general_merchandise, home_and_diy")
    p_search.add_argument("--max-results", type=int, default=20)
    p_search.set_defaults(func=cmd_search)

    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
