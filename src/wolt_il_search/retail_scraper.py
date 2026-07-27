"""Item scraper for Wolt retail venues (electronics, general merchandise, ...).

These venues use `loading_strategy: "partial"` in the consumer-assortment API
— category items aren't included in that JSON response at all, only the
category tree. The real per-category item data is embedded as SSR'd React
Query state in the venue's category page HTML (`/items/{category-slug}`),
which is what wolt.com's own frontend reads. There's no JSON endpoint for it
that doesn't also require this same page fetch, so this scraper parses the
`<script class="query-state">` blob out of each category page.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable

import requests

from .backoff import AdaptiveThrottle, HttpError, RetryPolicy, request_with_retry

_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_QUERY_STATE_RE = re.compile(
    r'<script type="application/json" class="query-state"[^>]*>(.*?)</script>', re.S
)
_CATEGORY_LINK_RE = re.compile(r'href="(/en/isr/[^"]*?/venue/[^"]*?/items/[^"]+)"')


class RetailScraperError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, permanent: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.permanent = permanent


class RetailScraper:
    def __init__(
        self,
        delay_seconds: float = 1.5,
        policy: RetryPolicy | None = None,
        on_retry: Callable[[dict], None] | None = None,
    ):
        # Pacing/retries are shared with WoltClient via backoff.py, but the
        # throttle instance is per-client: wolt.com (HTML) and
        # consumer-api.wolt.com (JSON) rate-limit independently, so one
        # getting throttled shouldn't slow the other down.
        self.policy = policy or RetryPolicy(base_delay=delay_seconds)
        self.throttle = AdaptiveThrottle(self.policy)
        self.on_retry = on_retry
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _UA, "Accept-Language": "en"})

    @property
    def delay_seconds(self) -> float:
        return self.throttle.delay

    def _get_html(self, url: str) -> str:
        try:
            resp = request_with_retry(
                lambda: self.session.get(url, timeout=25),
                describe=f"GET {url}",
                policy=self.policy,
                throttle=self.throttle,
                on_retry=self.on_retry,
            )
        except HttpError as e:
            # Same reasoning as WoltClient._get: don't let one flaky request
            # kill an hours-long unattended crawl.
            raise RetailScraperError(str(e), status_code=e.status_code, permanent=e.permanent) from e
        return resp.text

    @staticmethod
    def _extract_query_state(html: str) -> dict | None:
        m = _QUERY_STATE_RE.search(html)
        if not m:
            return None
        return json.loads(m.group(1))

    def get_category_paths(self, region_slug: str, venue_slug: str) -> list[str]:
        url = f"https://wolt.com/en/isr/{region_slug}/venue/{venue_slug}"
        html = self._get_html(url)
        return sorted(set(_CATEGORY_LINK_RE.findall(html)))

    def get_category_items(self, path: str) -> list[dict]:
        url = f"https://wolt.com{path}"
        html = self._get_html(url)
        qs = self._extract_query_state(html)
        if not qs:
            return []
        items: list[dict] = []
        for q in qs.get("queries", []):
            key = q.get("queryKey") or []
            if len(key) >= 2 and key[0] == "venue-assortment" and key[1] == "category":
                data = (q.get("state") or {}).get("data") or {}
                for page in data.get("pages", []):
                    category = page.get("category") or {}
                    for it in page.get("items", []):
                        it = dict(it)
                        it["_category_name"] = category.get("name")
                        items.append(it)
        return items

    def crawl_venue_items(self, region_slug: str, venue_slug: str) -> list[dict]:
        """All items across every category page for one venue, deduped by item id."""
        paths = self.get_category_paths(region_slug, venue_slug)
        by_id: dict[str, dict] = {}
        for path in paths:
            for it in self.get_category_items(path):
                item_id = it.get("id")
                if item_id:
                    by_id[item_id] = it
        return list(by_id.values())
