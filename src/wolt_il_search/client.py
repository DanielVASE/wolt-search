"""Thin client for Wolt's public, unauthenticated consumer endpoints.

These are the same endpoints wolt.com's own web app calls — no API key exists
for them. Wolt's ToS disallows automated access at scale; this client is
built for a single personal user running an occasional, rate-limited,
resumable crawl, not high-frequency scraping.
"""

from __future__ import annotations

import random
import re
import time

import requests

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
    pass


class WoltClient:
    def __init__(self, delay_seconds: float = 1.2, language: str = "en"):
        self.delay_seconds = delay_seconds
        self.language = language
        self.session = requests.Session()
        self.session.headers.update(_HEADERS)
        self._last_request = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request
        wait = self.delay_seconds - elapsed + random.uniform(0, 0.4)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def _get(self, path: str, params: dict | None = None) -> dict:
        self._throttle()
        url = f"{BASE_URL}{path}"
        headers = {"Accept-Language": self.language}
        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code == 429:
                time.sleep(5 + random.uniform(0, 3))
                self._throttle()
                resp = self.session.get(url, params=params, headers=headers, timeout=20)
        except requests.exceptions.RequestException as e:
            # Network hiccups (timeouts, connection resets) must not kill an
            # hours-long unattended crawl over one flaky request — surface as
            # a per-item WoltClientError so callers skip and move on.
            raise WoltClientError(f"GET {url} -> {e}") from e
        if not resp.ok:
            raise WoltClientError(f"GET {url} -> {resp.status_code}: {resp.text[:200]}")
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
        venues: list[dict] = []
        for section in data.get("sections", []):
            for item in section.get("items", []):
                venue = item.get("venue")
                if venue:
                    venues.append(venue)
        return venues

    def get_assortment(self, slug: str) -> dict:
        if not _SLUG_RE.fullmatch(slug):
            raise WoltClientError(f"invalid venue slug: {slug!r}")
        return self._get(f"/consumer-api/consumer-assortment/v1/venues/slug/{slug}/assortment")

    def list_venue_list(self, target: str, lat: float, lon: float) -> list[dict]:
        """Venues under a Wolt front-page section target, e.g. 'isr_retail_gm:tel-aviv'.

        This is how non-restaurant verticals (electronics, general merchandise,
        groceries, ...) are organized — they don't show up in
        `list_venues_near`, which only covers the restaurant vertical.
        """
        data = self._get(f"/v1/pages/venue-list/{target}", params={"lat": lat, "lon": lon})
        venues: list[dict] = []
        for section in data.get("sections", []):
            for item in section.get("items", []):
                venue = item.get("venue")
                if venue:
                    venues.append(venue)
        return venues
