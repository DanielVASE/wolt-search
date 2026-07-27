"""MCP server exposing nationwide-Israel Wolt search over the local cache.

All tools here read the local SQLite cache only — they never hit Wolt live,
so search is instant and doesn't nudge Wolt's rate limits. Build/refresh the
cache first with `wolt-il-refresh crawl` (see README). `get_menu_live` is the
one exception: it fetches a single venue's current menu directly from Wolt,
for when you want live prices right before ordering.

The search tool goes through search_full(), the same entry point the web UI
uses, so an agent gets the identical filter set, honest total counts and
pagination rather than a truncated list.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from .cache import Cache
from .client import WoltClient, WoltClientError
from .indexer import parse_menu_items
from .search import (
    SORT_PRICE_ASC,
    SORT_PRICE_DESC,
    SORT_RATING,
    SORT_RELEVANCE,
    SearchFilters,
    search_full,
)

_SORTS = (SORT_RELEVANCE, SORT_RATING, SORT_PRICE_ASC, SORT_PRICE_DESC)

DB_PATH = os.environ.get("WOLT_IL_DB")

mcp = FastMCP("wolt-il-search")


def _cache() -> Cache:
    return Cache(DB_PATH)


@mcp.tool(
    name="search",
    description=(
        "Search Wolt venues and menu items across ALL of Israel at once, from the local cache "
        "(no address/location switching needed). Matches venue name, cuisine tags, description, "
        "and menu item names/descriptions with fuzzy matching (typo-tolerant, no need for an exact "
        "substring). Returns venues ranked by match quality, with rating and open/closed status, "
        "each optionally including its best-matching menu items with prices in minor units "
        "(e.g. 2390 = 23.90 ILS). Also returns the true total match count (so you can tell 20-of-24 "
        "from 20-of-1200 and page with offset), plus venue and item-category facets you can feed "
        "back in as include_venues / categories to drill down."
    ),
)
def search(
    query: Annotated[str, Field(description="Free-text search, e.g. a dish name, cuisine, or venue name", min_length=1)],
    regions: Annotated[
        list[str] | None,
        Field(description="Restrict to these Wolt region slugs from list_regions, e.g. ['tel-aviv', 'haifa']"),
    ] = None,
    only_open: Annotated[bool, Field(description="Only currently-online venues")] = False,
    min_rating: Annotated[float | None, Field(description="Minimum rating, 0-10 scale", ge=0, le=10)] = None,
    product_lines: Annotated[
        list[str] | None,
        Field(description="Restrict to these verticals, e.g. ['restaurant', 'electronics', 'general_merchandise', 'home_and_diy']"),
    ] = None,
    include_venues: Annotated[
        list[str] | None,
        Field(description="Only these venues — exact slug, or a substring of the venue name (from the venue_facets of a previous call)"),
    ] = None,
    exclude_venues: Annotated[
        list[str] | None,
        Field(description="Drop these venues — exact slug or venue-name substring. Useful to filter out a chain that floods the results"),
    ] = None,
    categories: Annotated[
        list[str] | None,
        Field(description="Only items in these item categories, exact match (from the category_facets of a previous call)"),
    ] = None,
    min_price: Annotated[float | None, Field(description="Minimum item price in ILS (not minor units)", ge=0)] = None,
    max_price: Annotated[float | None, Field(description="Maximum item price in ILS (not minor units)", ge=0)] = None,
    sort: Annotated[
        str,
        Field(description="Ordering: 'relevance' (default), 'rating', 'price_asc', or 'price_desc'"),
    ] = SORT_RELEVANCE,
    max_results: Annotated[int, Field(description="Max venues to return", ge=1, le=100)] = 20,
    offset: Annotated[int, Field(description="Skip this many venues, for paging through a large result set", ge=0)] = 0,
) -> dict[str, Any]:
    if sort not in _SORTS:
        raise ValueError(f"sort must be one of {', '.join(_SORTS)}")
    filters = SearchFilters(
        # Tuples because SearchFilters is hashable/frozen-ish by convention, and
        # None (not an empty tuple) is what means "no constraint" downstream.
        regions=tuple(regions) if regions else None,
        only_open=only_open,
        min_rating=min_rating,
        product_lines=tuple(product_lines) if product_lines else None,
        include_venues=tuple(include_venues) if include_venues else None,
        exclude_venues=tuple(exclude_venues) if exclude_venues else None,
        categories=tuple(categories) if categories else None,
        min_price=min_price,
        max_price=max_price,
        sort=sort,
    )
    cache = _cache()
    try:
        response = search_full(cache, query, filters, offset=offset, limit=max_results)
        return {
            "results": [dataclasses.asdict(r) for r in response.results],
            "total": response.total,
            "offset": response.offset,
            "limit": response.limit,
            "venue_facets": [dataclasses.asdict(f) for f in response.venue_facets],
            "category_facets": [dataclasses.asdict(f) for f in response.category_facets],
        }
    finally:
        cache.close()


@mcp.tool(
    name="list_regions",
    description="List Wolt's delivery regions in Israel (slug + name + center coordinates). Use a slug with search(region=...) to narrow to one area.",
)
def list_regions() -> list[dict[str, Any]]:
    cache = _cache()
    try:
        return [dict(r) for r in cache.list_regions()]
    finally:
        cache.close()


@mcp.tool(
    name="cache_status",
    description=(
        "Show how much of Israel is indexed: region/venue/item counts, how many venues still need "
        "fetching, and a per-product-line breakdown. Useful to sanity-check coverage before trusting "
        "a search that came back thin — e.g. 'no pet shops matched' may just mean pet_supply isn't "
        "crawled yet. 'empty' counts venues fetched successfully that genuinely list no items, which "
        "is why with_items can be lower than venues even at 0 pending."
    ),
)
def cache_status() -> dict[str, Any]:
    cache = _cache()
    try:
        return {**cache.counts(), "product_lines": cache.product_line_stats()}
    finally:
        cache.close()


@mcp.tool(
    name="get_menu_live",
    description=(
        "Fetch a venue's CURRENT menu directly from Wolt (bypasses the cache) by slug. "
        "Use this right before ordering, once search() has narrowed down a venue, to get live prices "
        "and availability rather than the possibly-stale cached snapshot."
    ),
)
def get_menu_live(
    slug: Annotated[str, Field(description="Venue slug, from a search() result", min_length=2)],
) -> dict[str, Any]:
    client = WoltClient()
    try:
        data = client.get_assortment(slug)
    except WoltClientError as exc:
        # Distinguish "this venue is gone / the slug is wrong" from "Wolt is
        # rate-limiting or flaking", so the caller knows whether retrying or
        # re-running search() is the right next move. The client already
        # exhausted its own backoff before raising.
        raise ValueError(
            f"Could not fetch live menu for {slug!r}: {exc}"
            + (" (venue looks gone or the slug is wrong — try search() again)" if exc.permanent else " (transient — retry shortly)")
        ) from exc
    finally:
        # One-shot call, so don't leave the pooled connection open for the
        # lifetime of the MCP process.
        client.session.close()
    return {
        "slug": slug,
        "assortment_id": data.get("assortment_id"),
        "selected_language": data.get("selected_language"),
        "items": parse_menu_items(data),
    }


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
