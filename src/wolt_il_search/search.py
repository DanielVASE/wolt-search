"""Search over the local Wolt Israel cache.

Primary path: SQLite FTS5 with BM25 ranking (see cache.py's venues_fts /
items_fts). This is token-based, not character-fuzzy — a query for "RTX" only
matches documents that actually contain the token "rtx", which is exactly
what a character-similarity scorer (rapidfuzz) gets wrong on short technical
tokens: "gpu" and "Guido" look alike character-for-character but share no
tokens, and short/rare tokens should dominate ranking over common venue-name
words. Each query token is used as a prefix match (`token*`) so partial model
numbers still hit (e.g. "5070" matches an item token "5070ti"). Multi-token
queries try AND across tokens first (precision), then relax to OR if that
finds nothing (recall) — bm25 naturally ranks docs matching more terms higher
either way.

Fallback path: the FTS index is exact-token (modulo the prefix match), so it
has no typo tolerance — "Rizen" won't prefix-match "ryzen". When the FTS pass
finds literally nothing, we fall back to a rapidfuzz character-similarity
scan as a safety net for misspellings.

Faceted filtering: candidates are collected once with only the "base" filters
applied (region, open, rating, product_line, price). Facets (which shops and
item categories actually appear in that set) are computed from it before the
shop-selection and category filters narrow it further — so toggling a shop
in/out doesn't make it disappear from its own facet list, and the category
list always shows every category available at the current price/region/etc.
scope, not just the ones you've already picked.

Result completeness: every candidate the FTS query matches is fetched (no
arbitrary truncation before ranking — a prior version capped this at 500 and
silently dropped real matches for common terms), filtered, and counted.
`SearchResponse.total` is the true count of matching venues after every
filter; `results` is just the requested page of it.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field

from rapidfuzz import fuzz

from .cache import Cache

FUZZY_VENUE_MATCH_THRESHOLD = 65
FUZZY_ITEM_MATCH_THRESHOLD = 65
DEFAULT_ITEMS_PER_VENUE = 5
FACET_LIMIT = 40
# High enough to never truncate real matches at this dataset's scale
# (~7k venues, ~400k items) — Python-side sort/filter over a few thousand
# rows is well under 100ms, so there's no real cost to just fetching them all.
FTS_CANDIDATE_LIMIT = 50_000

WOLT_BASE_URL = "https://wolt.com/en/isr"

SORT_RELEVANCE = "relevance"
SORT_RATING = "rating"
SORT_PRICE_ASC = "price_asc"
SORT_PRICE_DESC = "price_desc"


def venue_url(region_slug: str, slug: str) -> str:
    return f"{WOLT_BASE_URL}/{region_slug}/venue/{slug}"


def item_url(region_slug: str, venue_slug: str, item_id: str) -> str:
    # Wolt 301-redirects to the item's actual region if this one's wrong, so
    # this resolves correctly even for venues whose cached region_slug is
    # stale (multi-region "warehouse" venues can have this — see indexer.py).
    return f"{WOLT_BASE_URL}/{region_slug}/venue/{venue_slug}/itemid-{item_id}"


def normalize(text: str | None) -> str:
    if not text:
        return ""
    # Strip Hebrew niqqud/cantillation marks and other combining marks, then
    # fold to lowercase and collapse whitespace/punctuation noise. This also
    # doubles as query sanitization before building an FTS5 MATCH string —
    # the tokens that come out are plain alphanumerics, safe to use as
    # bareword prefix queries without worrying about FTS5 query syntax.
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _tokenize(query: str) -> list[str]:
    return [t for t in normalize(query).split(" ") if t]


def _fts_match_query(tokens: list[str], op: str) -> str:
    # Quoting each token forces FTS5 to treat it as a literal string rather
    # than a query-syntax keyword — without this, a query like "not pizza"
    # turns into an actual NOT operator and silently inverts the match.
    return f" {op} ".join(f'"{tok}"*' for tok in tokens)


@dataclass
class ItemMatch:
    id: str
    name: str
    price: int | None
    category_name: str | None
    score: float
    wolt_url: str


@dataclass
class VenueResult:
    slug: str
    name: str
    region_slug: str
    online: bool
    rating: float | None
    address: str | None
    tags: list[str]
    short_description: str | None
    product_line: str | None
    score: float
    wolt_url: str
    matched_items: list[ItemMatch] = field(default_factory=list)
    total_matched_items: int = 0


@dataclass
class Facet:
    value: str
    label: str
    count: int


@dataclass
class SearchFilters:
    regions: tuple[str, ...] | None = None
    only_open: bool = False
    min_rating: float | None = None
    product_lines: tuple[str, ...] | None = None
    include_venues: tuple[str, ...] | None = None  # slug exact-match OR substring-on-name
    exclude_venues: tuple[str, ...] | None = None
    categories: tuple[str, ...] | None = None  # item category_name, exact match, OR'd
    min_price: float | None = None  # ILS
    max_price: float | None = None  # ILS
    sort: str = SORT_RELEVANCE
    items_per_venue: int = DEFAULT_ITEMS_PER_VENUE


@dataclass
class SearchResponse:
    results: list[VenueResult]
    total: int
    offset: int
    limit: int
    venue_facets: list[Facet] = field(default_factory=list)
    category_facets: list[Facet] = field(default_factory=list)


@dataclass
class _Candidate:
    venue_row: object
    item_rows: list
    text_score: float


def _passes_base_filters(venue_row, f: SearchFilters) -> bool:
    if f.regions and venue_row["region_slug"] not in f.regions:
        return False
    if f.only_open and not venue_row["online"]:
        return False
    if f.min_rating is not None and (venue_row["rating"] or 0) < f.min_rating:
        return False
    if f.product_lines and (venue_row["product_line"] or "") not in f.product_lines:
        return False
    return True


def _price_in_range(price: int | None, f: SearchFilters) -> bool:
    if price is None:
        return f.min_price is None and f.max_price is None
    ils = price / 100
    if f.min_price is not None and ils < f.min_price:
        return False
    if f.max_price is not None and ils > f.max_price:
        return False
    return True


def _venue_term_matches(venue_row, term: str) -> bool:
    term = term.lower()
    return term == (venue_row["slug"] or "").lower() or term in (venue_row["name"] or "").lower()


def _passes_venue_selection(venue_row, f: SearchFilters) -> bool:
    if f.include_venues and not any(_venue_term_matches(venue_row, t) for t in f.include_venues):
        return False
    if f.exclude_venues and any(_venue_term_matches(venue_row, t) for t in f.exclude_venues):
        return False
    return True


def _category_matching_items(item_rows: list, f: SearchFilters) -> list:
    if not f.categories:
        return item_rows
    return [r for r in item_rows if (r["category_name"] or "") in f.categories]


def _item_match(row, region_slug: str, venue_slug: str) -> ItemMatch:
    return ItemMatch(
        id=row["id"],
        name=row["name"],
        price=row["price"],
        category_name=row["category_name"],
        score=row["text_score"] if "text_score" in row.keys() else 0.0,
        wolt_url=item_url(region_slug, venue_slug, row["id"]),
    )


def _build_result(venue_row, score: float, matched_items: list[ItemMatch], total_matched_items: int) -> VenueResult:
    return VenueResult(
        slug=venue_row["slug"],
        name=venue_row["name"],
        region_slug=venue_row["region_slug"],
        online=bool(venue_row["online"]),
        rating=venue_row["rating"],
        address=venue_row["address"],
        tags=json.loads(venue_row["tags"] or "[]"),
        short_description=venue_row["short_description"],
        product_line=venue_row["product_line"],
        score=score,
        wolt_url=venue_url(venue_row["region_slug"], venue_row["slug"]),
        matched_items=matched_items,
        total_matched_items=total_matched_items,
    )


def _sort_key(f: SearchFilters):
    if f.sort == SORT_RATING:
        return lambda r: (r.rating or 0, r.score)
    if f.sort == SORT_PRICE_ASC:
        return lambda r: (min((it.price for it in r.matched_items if it.price is not None), default=float("inf")),)
    if f.sort == SORT_PRICE_DESC:
        return lambda r: (
            -min((it.price for it in r.matched_items if it.price is not None), default=float("-inf")),
        )
    return lambda r: (r.score,)


def _apply_sort(results: list[VenueResult], f: SearchFilters) -> None:
    reverse = f.sort != SORT_PRICE_ASC
    results.sort(key=_sort_key(f), reverse=reverse)


def _collect_fts_candidates(cache: Cache, tokens: list[str], f: SearchFilters) -> list[_Candidate]:
    venue_rows = cache.search_venues_fts(_fts_match_query(tokens, "AND"), limit=FTS_CANDIDATE_LIMIT)
    if not venue_rows:
        venue_rows = cache.search_venues_fts(_fts_match_query(tokens, "OR"), limit=FTS_CANDIDATE_LIMIT)
    venue_text_score: dict[str, float] = {row["slug"]: row["text_score"] for row in venue_rows}
    venue_row_by_slug = {row["slug"]: row for row in venue_rows}

    item_rows_all = cache.search_items_fts(_fts_match_query(tokens, "AND"), limit=FTS_CANDIDATE_LIMIT)
    if not item_rows_all:
        item_rows_all = cache.search_items_fts(_fts_match_query(tokens, "OR"), limit=FTS_CANDIDATE_LIMIT)

    items_by_venue: dict[str, list] = {}
    price_filter_active = f.min_price is not None or f.max_price is not None
    for row in item_rows_all:
        if price_filter_active and not _price_in_range(row["price"], f):
            continue
        items_by_venue.setdefault(row["venue_slug"], []).append(row)

    candidates: list[_Candidate] = []
    for slug in set(venue_text_score) | set(items_by_venue):
        venue_row = venue_row_by_slug.get(slug) or cache.get_venue(slug)
        if venue_row is None or not _passes_base_filters(venue_row, f):
            continue

        item_rows = items_by_venue.get(slug, [])
        if price_filter_active and not item_rows:
            continue  # price filter active and nothing at this venue falls in range

        item_score = item_rows[0]["text_score"] if item_rows else 0.0
        text_score = max(venue_text_score.get(slug, 0.0), item_score)
        candidates.append(_Candidate(venue_row=venue_row, item_rows=item_rows, text_score=text_score))

    return candidates


# -- rapidfuzz fallback (typo tolerance when FTS finds nothing) -------------


def _venue_field_score(norm_query: str, venue) -> float:
    name_score = fuzz.WRatio(norm_query, normalize(venue["name"]))
    tags = json.loads(venue["tags"] or "[]")
    tag_score = max((fuzz.WRatio(norm_query, normalize(t)) for t in tags), default=0.0)
    # partial_ratio against free-text descriptions is deliberately excluded here:
    # it looks for the best-aligned substring of the query's own length, which
    # spuriously matches short queries (e.g. "ryzen") against unrelated long
    # descriptions. Only score structured, short fields for the fuzzy fallback.
    return max(name_score, tag_score * 0.9)


def _fuzzy_item_score(norm_query: str, it) -> float:
    name_score = fuzz.WRatio(norm_query, normalize(it["name"]))
    cat_score = fuzz.WRatio(norm_query, normalize(it["category_name"])) * 0.8
    return max(name_score, cat_score)


def _collect_fuzzy_candidates(cache: Cache, norm_query: str, f: SearchFilters) -> list[_Candidate]:
    items_by_venue: dict[str, list] = {}
    for row in cache.all_menu_items():
        items_by_venue.setdefault(row["venue_slug"], []).append(row)

    price_filter_active = f.min_price is not None or f.max_price is not None
    candidates: list[_Candidate] = []
    for venue_row in cache.all_venues():
        if not _passes_base_filters(venue_row, f):
            continue

        venue_score = _venue_field_score(norm_query, venue_row)
        scored_items = []
        for it in items_by_venue.get(venue_row["slug"], []):
            if not it["enabled"] or not _price_in_range(it["price"], f):
                continue
            score = _fuzzy_item_score(norm_query, it)
            if score >= FUZZY_ITEM_MATCH_THRESHOLD:
                row = dict(it)
                row["text_score"] = score
                scored_items.append(row)
        scored_items.sort(key=lambda r: r["text_score"], reverse=True)

        if price_filter_active and not scored_items:
            continue
        item_score = scored_items[0]["text_score"] if scored_items else 0.0
        combined = max(venue_score, item_score)
        if combined < FUZZY_VENUE_MATCH_THRESHOLD:
            continue
        candidates.append(_Candidate(venue_row=venue_row, item_rows=scored_items, text_score=combined))

    return candidates


# -- shared candidate -> facets/results pipeline -----------------------------


def _compute_facets(candidates: list[_Candidate]) -> tuple[list[Facet], list[Facet]]:
    venue_facets = [
        Facet(value=c.venue_row["slug"], label=c.venue_row["name"], count=len(c.item_rows)) for c in candidates
    ]
    venue_facets.sort(key=lambda fa: (-fa.count, fa.label or ""))

    category_counter: Counter[str] = Counter()
    for c in candidates:
        for it in c.item_rows:
            if it["category_name"]:
                category_counter[it["category_name"]] += 1
    category_facets = [Facet(value=name, label=name, count=cnt) for name, cnt in category_counter.most_common(FACET_LIMIT)]

    return venue_facets[:FACET_LIMIT], category_facets


def _finalize(candidates: list[_Candidate], f: SearchFilters, offset: int, limit: int) -> SearchResponse:
    venue_facets, category_facets = _compute_facets(candidates)

    results: list[VenueResult] = []
    for c in candidates:
        if not _passes_venue_selection(c.venue_row, f):
            continue
        matching_items = _category_matching_items(c.item_rows, f)
        if f.categories and not matching_items:
            continue

        region_slug = c.venue_row["region_slug"]
        slug = c.venue_row["slug"]
        matched_items = [_item_match(r, region_slug, slug) for r in matching_items[: f.items_per_venue]]
        results.append(_build_result(c.venue_row, c.text_score, matched_items, len(matching_items)))

    _apply_sort(results, f)
    total = len(results)
    page = results[offset : offset + limit]
    return SearchResponse(
        results=page, total=total, offset=offset, limit=limit, venue_facets=venue_facets, category_facets=category_facets
    )


def search(
    cache: Cache,
    query: str,
    region: str | None = None,
    only_open: bool = False,
    min_rating: float | None = None,
    product_line: str | None = None,
    max_results: int = 20,
) -> list[VenueResult]:
    """Back-compat single-region/single-category convenience wrapper. New
    code (the web UI) should use search_full() for facets, pagination, and
    honest total counts.
    """
    filters = SearchFilters(
        regions=(region,) if region else None,
        only_open=only_open,
        min_rating=min_rating,
        product_lines=(product_line,) if product_line else None,
    )
    response = search_full(cache, query, filters, offset=0, limit=max_results)
    return response.results


def search_full(
    cache: Cache,
    query: str,
    filters: SearchFilters | None = None,
    offset: int = 0,
    limit: int = 20,
) -> SearchResponse:
    f = filters or SearchFilters()
    norm_query = normalize(query)
    tokens = _tokenize(query)
    if not tokens:
        return SearchResponse(results=[], total=0, offset=offset, limit=limit)

    candidates = _collect_fts_candidates(cache, tokens, f)
    if not candidates:
        candidates = _collect_fuzzy_candidates(cache, norm_query, f)

    return _finalize(candidates, f, offset, limit)
