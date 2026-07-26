"""MCP server exposing nationwide-Israel Wolt search over the local cache.

All tools here read the local SQLite cache only — they never hit Wolt live,
so search is instant and doesn't nudge Wolt's rate limits. Build/refresh the
cache first with `wolt-il-refresh full` (see README). `get_menu_live` is the
one exception: it fetches a single venue's current menu directly from Wolt,
for when you want live prices right before ordering.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from .cache import Cache
from .client import WoltClient
from .indexer import parse_menu_items
from .search import search as run_search

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
        "(e.g. 2390 = 23.90 ILS)."
    ),
)
def search(
    query: Annotated[str, Field(description="Free-text search, e.g. a dish name, cuisine, or venue name", min_length=1)],
    region: Annotated[
        str | None, Field(description="Restrict to one Wolt region slug from list_regions, e.g. 'tel-aviv'")
    ] = None,
    only_open: Annotated[bool, Field(description="Only currently-online venues")] = False,
    min_rating: Annotated[float | None, Field(description="Minimum rating, 0-10 scale", ge=0, le=10)] = None,
    product_line: Annotated[
        str | None,
        Field(description="Restrict to one vertical, e.g. 'restaurant', 'electronics', 'general_merchandise', 'home_and_diy'"),
    ] = None,
    max_results: Annotated[int, Field(description="Max venues to return", ge=1, le=100)] = 20,
) -> list[dict[str, Any]]:
    cache = _cache()
    try:
        results = run_search(
            cache,
            query,
            region=region,
            only_open=only_open,
            min_rating=min_rating,
            product_line=product_line,
            max_results=max_results,
        )
        return [dataclasses.asdict(r) for r in results]
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
    description="Show how much of Israel is indexed: region/venue/menu counts, useful to sanity-check before relying on search results.",
)
def cache_status() -> dict[str, Any]:
    cache = _cache()
    try:
        return cache.counts()
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
    data = client.get_assortment(slug)
    items = parse_menu_items(data)
    return {
        "slug": slug,
        "assortment_id": data.get("assortment_id"),
        "selected_language": data.get("selected_language"),
        "items": items,
    }


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
