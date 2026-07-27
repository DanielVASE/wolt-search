"""Crawlable category catalog, discovered from Wolt's own API.

The admin UI used to offer a fixed list of eleven hand-written jobs, with the
verticals baked in as module constants (`isr_retail_gm`, `g_retail_groceries`)
and the item-fetch scopes baked into argv strings
(`--product-lines general_merchandise,home_and_diy`). Two consequences:

* adding a vertical meant editing three files, and
* five product lines that Wolt actually returns (florist, pharmacy,
  pet_supply, toys_games_and_kids, health_and_beauty — 839 venues) had no
  job targeting them at all, so their item catalogs could never be crawled
  from the UI.

So the catalog is built at runtime from three sources:

1. **Verticals** — `/v1/pages/front` section links with
   `view_name: "venue_list"` (e.g. `isr_retail_gm` "Stores Around You",
   `g_retail_groceries` "Fill your fridge"). These are venue *discovery*
   targets: feed them to `/v1/pages/venue-list/{target}:{region}`.
2. **Wolt categories** — the same page's `category-list` section, whose links
   use `view_name: "venue_category"` and targets like
   `category-sushi:tel-aviv`. Same endpoint, narrower slice.
3. **Product lines** — the `product_line` values Wolt stamped on venues we've
   already cached. These are what an *item* crawl is scoped by, and they're
   discovered rather than enumerated, so a brand-new product line becomes
   selectable the moment one venue carrying it is cached.

Discovery is cached in the `meta` table so rendering the admin page never
depends on a live Wolt request.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field

from .cache import Cache
from .client import WoltClient

# Row key in the cache's `meta` table, not a credential — the trailing _v1 lets
# a future schema change invalidate the cached catalog by bumping the key.
DISCOVERY_META_KEY = "wolt_categories_v1"  # gitleaks:allow
DISCOVERY_TTL_SECONDS = 24 * 3600

# The restaurant vertical isn't a front-page venue-list target — it has its
# own endpoint (`/v1/pages/restaurants`), so it's the one entry that has to be
# named explicitly rather than discovered.
RESTAURANTS_KEY = "restaurants"

# Item-fetch strategies:
#   json   — /consumer-api/.../assortment returns items directly (cheap).
#   scrape — venue uses loading_strategy="partial", so the JSON assortment is
#            always empty and only the SSR'd HTML category pages have items.
#   auto   — try json, then HTML-scrape whatever came back empty. This is the
#            right default for any *unknown* product line: Wolt mixes loading
#            strategies within a single product line, and the old code only
#            ran the fallback for two hardcoded lines.
STRATEGY_JSON = "json"
STRATEGY_SCRAPE = "scrape"
STRATEGY_AUTO = "auto"

# Product lines known to be 100% loading_strategy="partial" — going through
# the JSON path first would just be a wasted request per venue.
ALWAYS_SCRAPE_PRODUCT_LINES = frozenset({"electronics", "grocery"})

# Verticals we always want offered even before/without a successful live
# discovery call, so the admin page is never empty and an offline dev box
# still works. Discovery *adds* to this; it doesn't replace it.
BUILTIN_VERTICALS: tuple[dict, ...] = (
    {"target": RESTAURANTS_KEY, "title": "Restaurants & food", "primary": True},
    {"target": "isr_retail_gm", "title": "Stores Around You (retail)", "primary": True},
    {"target": "g_retail_groceries", "title": "Groceries & convenience", "primary": True},
)

# Front-page sections that are editorial/rotating slices of venues we already
# reach through a primary vertical ("Popular right now", "New on Wolt",
# "From 15% off"). Crawling them adds requests without adding venues, so
# they're offered but flagged so the UI can hide them by default.
_CURATED_HINTS = ("top-", "-picks", "newest-", "hot-", "most-", "top_", "offers", "nightoffers", "always_on")


@dataclass
class CrawlCategory:
    """One selectable row in the admin category picker."""

    key: str
    label: str
    kind: str  # 'vertical' | 'wolt_category' | 'product_line'
    #: venue-list target for the discovery phase (None for product lines,
    #: which are discovered as a side effect of a vertical crawl)
    discovery_target: str | None = None
    #: product_line values an item crawl for this category should cover
    product_lines: tuple[str, ...] = ()
    item_strategy: str = STRATEGY_AUTO
    primary: bool = False
    curated: bool = False
    source: str = "builtin"  # 'builtin' | 'wolt' | 'cache'
    # Live cache stats, filled in by build_catalog()
    venues: int = 0
    with_items: int = 0
    pending: int = 0

    def to_json(self) -> dict:
        data = asdict(self)
        data["product_lines"] = list(self.product_lines)
        return data


def strategy_for_product_lines(product_lines: tuple[str, ...]) -> str:
    """Pick the cheapest strategy that can actually return items."""
    if product_lines and all(pl in ALWAYS_SCRAPE_PRODUCT_LINES for pl in product_lines):
        return STRATEGY_SCRAPE
    return STRATEGY_AUTO


def _is_curated(target: str) -> bool:
    lowered = target.lower()
    return any(hint in lowered for hint in _CURATED_HINTS)


def discover(client: WoltClient, cache: Cache, force: bool = False) -> dict:
    """Fetch Wolt's live vertical/category list for the first cached region.

    Cached in `meta` for DISCOVERY_TTL_SECONDS — one region's front page is
    representative of the taxonomy (the per-region suffix is stripped), and
    the admin page shouldn't make a network call on every render.
    """
    if not force:
        raw = cache.get_meta(DISCOVERY_META_KEY)
        if raw:
            try:
                cached = json.loads(raw)
                if time.time() - cached.get("fetched_at", 0) < DISCOVERY_TTL_SECONDS:
                    return cached
            except (ValueError, AttributeError):
                pass

    regions = cache.list_regions()
    if not regions:
        return {"fetched_at": 0, "verticals": [], "categories": [], "error": "no regions cached yet"}

    anchor = regions[0]
    try:
        found = client.list_front_page_targets(anchor["lat"], anchor["lon"])
    except Exception as e:  # noqa: BLE001 - discovery is best-effort by design
        return {"fetched_at": 0, "verticals": [], "categories": [], "error": str(e)}

    payload = {
        "fetched_at": time.time(),
        "region": anchor["slug"],
        "verticals": [f for f in found if f["kind"] == "vertical"],
        "categories": [f for f in found if f["kind"] == "category"],
    }
    cache.set_meta(DISCOVERY_META_KEY, json.dumps(payload, ensure_ascii=False))
    return payload


def build_catalog(cache: Cache, discovery: dict | None = None) -> list[CrawlCategory]:
    """Merge live Wolt discovery with what's in the cache into one picker list."""
    discovery = discovery or {}
    stats = cache.product_line_stats()
    by_line = {row["product_line"]: row for row in stats}

    catalog: list[CrawlCategory] = []
    seen: set[str] = set()

    def add(cat: CrawlCategory) -> None:
        if cat.key in seen:
            return
        seen.add(cat.key)
        catalog.append(cat)

    # 1. Verticals — builtin first so they keep their curated labels/order.
    for spec in BUILTIN_VERTICALS:
        add(
            CrawlCategory(
                key=spec["target"],
                label=spec["title"],
                kind="vertical",
                discovery_target=None if spec["target"] == RESTAURANTS_KEY else spec["target"],
                primary=spec.get("primary", False),
                source="builtin",
            )
        )
    for spec in discovery.get("verticals", []):
        target = spec.get("target")
        if not target:
            continue
        add(
            CrawlCategory(
                key=target,
                label=spec.get("title") or target,
                kind="vertical",
                discovery_target=target,
                curated=_is_curated(target),
                source="wolt",
            )
        )

    # 2. Wolt's own category taxonomy (cuisine/type slices).
    for spec in discovery.get("categories", []):
        target = spec.get("target")
        if not target:
            continue
        add(
            CrawlCategory(
                key=target,
                label=spec.get("title") or target,
                kind="wolt_category",
                discovery_target=target,
                source="wolt",
            )
        )

    # 3. Product lines actually present in the cache — the item-crawl scopes.
    for line, row in by_line.items():
        if not line:
            continue
        add(
            CrawlCategory(
                key=f"pl:{line}",
                label=line.replace("_", " "),
                kind="product_line",
                product_lines=(line,),
                item_strategy=strategy_for_product_lines((line,)),
                source="cache",
                venues=row["total"],
                with_items=row["with_items"],
                pending=row["pending"],
            )
        )

    # Roll cache stats up onto the verticals/categories too, so the picker can
    # show "1,331 pending" against a vertical rather than only against the
    # product lines underneath it.
    vertical_lines = cache.product_lines_by_discovery_target()
    for cat in catalog:
        if cat.kind == "product_line":
            continue
        lines = vertical_lines.get(cat.key)
        if cat.key == RESTAURANTS_KEY and not lines:
            lines = ("restaurant",)
        if not lines:
            continue
        cat.product_lines = tuple(lines)
        cat.item_strategy = strategy_for_product_lines(cat.product_lines)
        cat.venues = sum(by_line.get(pl, {}).get("total", 0) for pl in lines)
        cat.with_items = sum(by_line.get(pl, {}).get("with_items", 0) for pl in lines)
        cat.pending = sum(by_line.get(pl, {}).get("pending", 0) for pl in lines)

    return catalog


@dataclass
class CrawlPlan:
    """What a single crawl job will actually do, in execution order."""

    discovery_targets: list[tuple[str, str]] = field(default_factory=list)  # (key, target|'restaurants')
    item_groups: list[tuple[str, tuple[str, ...], str]] = field(default_factory=list)  # (label, lines, strategy)

    @property
    def is_empty(self) -> bool:
        return not self.discovery_targets and not self.item_groups


def plan_crawl(catalog: list[CrawlCategory], keys: list[str], phases: tuple[str, ...]) -> CrawlPlan:
    """Turn a picker selection into an ordered, de-duplicated crawl plan.

    De-duplication matters: `isr_retail_gm` is the discovery target for eight
    product lines, so selecting several retail categories must not re-run the
    same 24-region discovery sweep eight times.
    """
    by_key = {c.key: c for c in catalog}
    plan = CrawlPlan()
    seen_targets: set[str] = set()
    scrape_lines: list[str] = []
    json_lines: list[str] = []
    auto_lines: list[str] = []

    for key in keys:
        cat = by_key.get(key)
        if cat is None:
            continue
        if "discover" in phases and cat.kind != "product_line":
            target = cat.discovery_target or RESTAURANTS_KEY
            if target not in seen_targets:
                seen_targets.add(target)
                plan.discovery_targets.append((cat.key, target))
        if "items" in phases:
            lines = cat.product_lines
            if not lines and cat.kind == "product_line":
                lines = (cat.key.removeprefix("pl:"),)
            for line in lines:
                bucket = (
                    scrape_lines
                    if line in ALWAYS_SCRAPE_PRODUCT_LINES
                    else (json_lines if cat.item_strategy == STRATEGY_JSON else auto_lines)
                )
                if line not in bucket:
                    bucket.append(line)

    # Cheap JSON path first, then the mixed auto path, then the slow scraper —
    # so a crawl produces visible results early instead of spending its first
    # hour on HTML pages.
    if json_lines:
        plan.item_groups.append(("items via JSON API", tuple(json_lines), STRATEGY_JSON))
    if auto_lines:
        plan.item_groups.append(("items via JSON API + scrape fallback", tuple(auto_lines), STRATEGY_AUTO))
    if scrape_lines:
        plan.item_groups.append(("items via HTML scrape", tuple(scrape_lines), STRATEGY_SCRAPE))
    return plan
