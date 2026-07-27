"""Thin client for Wolt's public, unauthenticated consumer endpoints.

These are the same endpoints wolt.com's own web app calls — no API key exists
for them. Wolt's ToS disallows automated access at scale; this client is
built for a single personal user running an occasional, rate-limited,
resumable crawl, not high-frequency scraping.

Pacing and retries live in backoff.py and are shared with retail_scraper.py
(they used to be duplicated, and neither did real exponential backoff).
"""

from __future__ import annotations

import re
from collections.abc import Callable

import requests

from .backoff import AdaptiveThrottle, HttpError, RetryPolicy, request_with_retry

BASE_URL = "https://consumer-api.wolt.com"

# Wolt slugs are lowercase alphanumeric-with-hyphens. get_menu_live's slug
# comes straight from an MCP caller, not Wolt's own API — reject anything
# else before it becomes part of a URL path (e.g. "../" traversal attempts).
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Origin": "https://wolt.com",
    "Referer": "https://wolt.com/",
    "x-platform": "web",
    "Accept": "application/json",
}


class WoltClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, permanent: bool = False):
        super().__init__(message)
        self.status_code = status_code
        # True when re-requesting can never succeed (404/410/403/...), so
        # callers can stop re-queueing the target instead of treating it the
        # same as a transient timeout. See indexer.py.
        self.permanent = permanent


class WoltClient:
    def __init__(
        self,
        delay_seconds: float = 1.2,
        language: str = "en",
        policy: RetryPolicy | None = None,
        on_retry: Callable[[dict], None] | None = None,
    ):
        self.policy = policy or RetryPolicy(base_delay=delay_seconds)
        self.throttle = AdaptiveThrottle(self.policy)
        # Called with a small dict per retry so a crawl can report "backing
        # off 8s after HTTP 429" instead of appearing to hang.
        self.on_retry = on_retry
        self.language = language
        self.session = requests.Session()
        self.session.headers.update(_HEADERS)

    @property
    def delay_seconds(self) -> float:
        """Current *adaptive* pacing, which starts at the configured base
        delay and widens while Wolt is pushing back."""
        return self.throttle.delay

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{BASE_URL}{path}"
        headers = {"Accept-Language": self.language}
        try:
            resp = request_with_retry(
                lambda: self.session.get(url, params=params, headers=headers, timeout=20),
                describe=f"GET {url}",
                policy=self.policy,
                throttle=self.throttle,
                on_retry=self.on_retry,
            )
        except HttpError as e:
            # Network hiccups and exhausted retries must not kill an
            # hours-long unattended crawl — surface as a per-item
            # WoltClientError so callers skip and move on.
            raise WoltClientError(str(e), status_code=e.status_code, permanent=e.permanent) from e
        return resp.json()

    def list_cities(self) -> list[dict]:
        """All Wolt cities/regions globally. Filter by country_code_alpha2 client-side."""
        data = self._get("/v1/cities")
        return data.get("results", [])

    def list_venues_near(self, lat: float, lon: float, radius: int = 40000) -> list[dict]:
        """Venues in the Wolt delivery region covering (lat, lon).

        Note: Wolt scopes this to one delivery region regardless of how large
        `radius` is — increasing it past the region's own extent adds nothing.
        Call this once per region anchor, not with an oversized radius from a
        single point, to cover a whole country.
        """
        data = self._get("/v1/pages/restaurants", params={"lat": lat, "lon": lon, "radius": radius})
        return _venues_from_sections(data)

    def get_assortment(self, slug: str) -> dict:
        if not _SLUG_RE.fullmatch(slug):
            raise WoltClientError(f"invalid venue slug: {slug!r}", permanent=True)
        return self._get(f"/consumer-api/consumer-assortment/v1/venues/slug/{slug}/assortment")

    def list_venue_list(self, target: str, lat: float, lon: float) -> list[dict]:
        """Venues under a Wolt front-page section target, e.g. 'isr_retail_gm:tel-aviv'.

        This is how non-restaurant verticals (electronics, general merchandise,
        groceries, ...) are organized — they don't show up in
        `list_venues_near`, which only covers the restaurant vertical.
        """
        data = self._get(f"/v1/pages/venue-list/{target}", params={"lat": lat, "lon": lon})
        return _venues_from_sections(data)

    def list_front_page_targets(self, lat: float, lon: float) -> list[dict]:
        """The verticals and categories Wolt itself advertises for a location.

        `/v1/pages/front` is the discovery endpoint wolt.com calls to build
        its homepage. Two things in it are crawlable via
        `/v1/pages/venue-list/{target}`:

        * each section's own `link` when `view_name == "venue_list"` — the
          verticals, e.g. `isr_retail_gm` ("Stores Around You") and
          `g_retail_groceries` ("Fill your fridge"). This is how the grocery
          vertical was found in the first place (see README); reading it live
          means a new Wolt vertical appears in the admin category picker
          instead of needing a new hardcoded constant.
        * the items of the `category-list` section, whose links use
          `view_name == "venue_category"` and targets like
          `category-sushi:tel-aviv` — Wolt's own category taxonomy.

        The per-region `":{slug}"` suffix is stripped so targets are
        comparable across regions (the crawler re-appends the region it's
        currently sweeping).
        """
        data = self._get("/v1/pages/front", params={"lat": lat, "lon": lon})
        found: dict[str, dict] = {}

        def record(target: object, title: object, kind: str) -> None:
            if not isinstance(target, str) or not target:
                return
            base = target.split(":", 1)[0].strip()
            # Skip deep links ("/v1/pages/...", "https://...") and bare venue
            # ObjectIds — neither is a crawlable venue-list target.
            if not base or base in found or not _CRAWLABLE_TARGET_RE.fullmatch(base):
                return
            if _OBJECT_ID_RE.fullmatch(base):
                return
            found[base] = {
                "target": base,
                "title": title if isinstance(title, str) and title.strip() else base,
                "kind": kind,
            }

        for section in data.get("sections", []):
            if not isinstance(section, dict):
                continue
            link = section.get("link") or {}
            if isinstance(link, dict) and link.get("view_name") == "venue_list":
                record(link.get("target"), section.get("title") or section.get("name"), "vertical")
            for item in section.get("items") or []:
                if not isinstance(item, dict):
                    continue
                item_link = item.get("link") or {}
                if not isinstance(item_link, dict):
                    continue
                view = item_link.get("view_name")
                if view == "venue_category":
                    record(item_link.get("target"), item.get("title"), "category")
                elif view == "venue_list":
                    record(item_link.get("target"), item.get("title"), "vertical")

        return list(found.values())


# Crawlable targets are slug-ish: lowercase words joined by - or _.
_CRAWLABLE_TARGET_RE = re.compile(r"[a-z0-9][a-z0-9_-]{2,}")
# 24-char hex = a Mongo ObjectId, i.e. one specific venue, not a category.
_OBJECT_ID_RE = re.compile(r"[0-9a-f]{24}")


def _venues_from_sections(data: dict) -> list[dict]:
    venues: list[dict] = []
    for section in data.get("sections", []):
        for item in section.get("items", []):
            venue = item.get("venue")
            if venue:
                venues.append(venue)
    return venues
