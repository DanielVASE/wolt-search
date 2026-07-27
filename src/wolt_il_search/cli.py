"""Command-line entry point for building/refreshing the local Wolt Israel cache.

Menu indexing means one HTTP request per venue (thousands, nationwide), so
this is deliberately a batched, resumable, rate-limited crawl you run
yourself (once, then periodically) rather than something triggered inline by
an MCP tool call.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from .backoff import RetryPolicy
from .cache import Cache
from .categories import (
    STRATEGY_AUTO,
    STRATEGY_JSON,
    STRATEGY_SCRAPE,
    build_catalog,
    discover,
    plan_crawl,
)
from .client import WoltClient
from .indexer import (
    DEFAULT_MENU_MAX_AGE_SECONDS,
    refresh_empty_catalog_venues,
    refresh_grocery_venues,
    refresh_menus,
    refresh_regions,
    refresh_retail_venues,
    refresh_scraped_items,
    refresh_venue_discovery,
    refresh_venues,
)
from .retail_scraper import RetailScraper
from .search import search as run_search

LOG = logging.getLogger("wolt_il_search.cli")


class Reporter:
    """Emits crawl progress as either human text or NDJSON events.

    The admin UI used to infer job state purely from `Popen.poll()`, so it
    could only ever say "running" or "exited (code 0)" — all the useful
    detail (which phase, which venue, how many of how many) was printed as
    prose into a log file that nothing parsed. In `json` mode every event is
    one self-describing line on stdout, which the webapp reads incrementally
    to drive real progress bars, and which still lands in the same log file
    for after-the-fact debugging.
    """

    def __init__(self, mode: str = "text") -> None:
        self.mode = mode
        self.started = time.time()
        self.phase_name: str | None = None
        self.total = 0
        self.done = 0
        self.ok = 0
        self.failed = 0
        self.items = 0

    # -- emission ---------------------------------------------------------
    def _emit(self, event: dict) -> None:
        if self.mode == "json":
            # Line-buffered single-line JSON: the reader tails this file and
            # must never see a half-written event.
            sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
            sys.stdout.flush()

    def _say(self, text: str) -> None:
        if self.mode == "json":
            self._emit({"ev": "log", "msg": text, "t": time.time()})
        else:
            print(text, flush=True)

    # -- lifecycle --------------------------------------------------------
    def plan(self, steps: list[str]) -> None:
        self._emit({"ev": "plan", "steps": steps, "t": time.time()})
        if self.mode != "json":
            print("plan: " + " → ".join(steps), flush=True)

    def phase(self, name: str, total: int | None = None, index: int = 0, of: int = 0) -> None:
        self.phase_name = name
        self.total = total or 0
        self.done = self.ok = self.failed = self.items = 0
        self._emit(
            {"ev": "phase", "name": name, "total": self.total, "index": index, "of": of, "t": time.time()}
        )
        if self.mode != "json":
            suffix = f" ({total} to do)" if total else ""
            print(f"\n== {name}{suffix}", flush=True)

    def progress(self, event: dict) -> None:
        """Sink for indexer `on_progress` callbacks."""
        kind = event.get("ev")
        if kind == "total":
            self.total = event.get("n", 0)
            self._emit({"ev": "phase_total", "total": self.total, "t": time.time()})
            if self.total == 0:
                # Say so explicitly. A phase that silently shows 0/0 is
                # indistinguishable from one that's broken.
                self._say("  nothing to do — no venues match this phase")
            return
        if event.get("state") == "start":
            # "currently working on X" — the thing the old UI could never show.
            self._emit(
                {
                    "ev": "current",
                    "label": event.get("label"),
                    "i": event.get("i"),
                    "n": event.get("n") or self.total,
                    "t": time.time(),
                }
            )
            return

        self.done += 1
        if event.get("ok"):
            self.ok += 1
            self.items += int(event.get("items") or event.get("found") or 0)
        else:
            self.failed += 1
        self._emit(
            {
                "ev": "tick",
                "label": event.get("label"),
                "i": event.get("i") or self.done,
                "n": event.get("n") or self.total,
                "ok": bool(event.get("ok")),
                "items": event.get("items") or event.get("found"),
                "error": event.get("error"),
                "permanent": event.get("permanent"),
                "done": self.done,
                "okc": self.ok,
                "failc": self.failed,
                "itemc": self.items,
                "t": time.time(),
            }
        )
        if self.mode != "json":
            label = event.get("label")
            if event.get("ok"):
                print(f"  {label}: {event.get('items', event.get('found', 0))} items", flush=True)
            else:
                print(f"  {label}: FAILED ({event.get('error', '')[:120]})", flush=True)

    def retry(self, info: dict) -> None:
        """Sink for backoff `on_retry` — makes waiting visible instead of
        looking like the job hung."""
        self._emit({"ev": "retry", **info, "t": time.time()})
        if self.mode != "json":
            print(
                f"  ! {info.get('reason')} — retry {info.get('attempt')}/{info.get('of')}"
                f" in {info.get('wait')}s (pacing {info.get('pacing')}s)",
                flush=True,
            )

    def finish(self, summary: dict) -> None:
        self._emit({"ev": "done", "summary": summary, "elapsed": time.time() - self.started, "t": time.time()})
        if self.mode != "json":
            print(f"\ndone in {time.time() - self.started:.0f}s: {summary}", flush=True)


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


def _policy(args: argparse.Namespace) -> RetryPolicy:
    return RetryPolicy(
        base_delay=args.delay,
        max_delay=getattr(args, "max_delay", 60.0),
        max_attempts=getattr(args, "max_attempts", 4),
    )


def cmd_crawl(args: argparse.Namespace) -> None:
    """One command for "crawl these categories", replacing the pick-one-of-
    eleven-jobs model the admin UI was built around.

    Everything is unbounded by default: `--limit 0` means "every remaining
    venue in the selected categories", so the operator no longer has to press
    Start repeatedly to get through a category.
    """
    cache = Cache(args.db)
    reporter = Reporter(args.progress)
    policy = _policy(args)
    client = WoltClient(policy=policy, on_retry=reporter.retry)
    scraper = RetailScraper(policy=policy, on_retry=reporter.retry)

    keys = [k.strip() for k in args.categories.split(",") if k.strip()]
    phases = tuple(p.strip() for p in args.phases.split(",") if p.strip())
    if not keys:
        raise SystemExit("--categories is required (comma-separated keys from /api/categories)")

    max_age_seconds = args.max_age_hours * 3600
    if args.force:
        # "Re-crawl everything I selected regardless of how fresh it is."
        max_age_seconds = 0.0

    # Regions are the anchor for every discovery sweep; fetch them first if
    # this is a cold cache instead of failing with "no regions cached".
    needs_regions = not cache.list_regions()
    discovery_meta = discover(client, cache) if not needs_regions else {}
    catalog = build_catalog(cache, discovery_meta)
    plan = plan_crawl(catalog, keys, phases)
    if plan.is_empty:
        raise SystemExit(f"nothing to do for categories={keys} phases={phases}")

    labels = {c.key: c.label for c in catalog}
    steps: list[str] = []
    if needs_regions:
        steps.append("regions")
    steps += [f"discover: {labels.get(key, key)}" for key, _ in plan.discovery_targets]
    steps += [label for label, _, _ in plan.item_groups]
    reporter.plan(steps)

    summary = {"venues_found": 0, "items_attempted": 0, "items_succeeded": 0}
    step_index = 0
    total_steps = len(steps)

    if needs_regions:
        step_index += 1
        reporter.phase("regions", total=1, index=step_index, of=total_steps)
        n = refresh_regions(client, cache)
        reporter.progress({"ev": "tick", "i": 1, "n": 1, "label": "regions", "ok": True, "found": n})
        # Now that regions exist, discovery can run for the category catalog.
        discovery_meta = discover(client, cache, force=True)

    region_count = len(cache.list_regions())
    for key, target in plan.discovery_targets:
        step_index += 1
        reporter.phase(f"discover: {labels.get(key, key)}", total=region_count, index=step_index, of=total_steps)
        found = refresh_venue_discovery(client, cache, target, on_progress=reporter.progress)
        summary["venues_found"] += found

    for label, lines, strategy in plan.item_groups:
        step_index += 1
        reporter.phase(f"{label} [{', '.join(lines)}]", index=step_index, of=total_steps)
        if strategy in (STRATEGY_JSON, STRATEGY_AUTO):
            attempted, succeeded = refresh_menus(
                client,
                cache,
                limit=args.limit,
                max_age_seconds=max_age_seconds,
                product_lines=lines,
                on_progress=reporter.progress,
            )
            summary["items_attempted"] += attempted
            summary["items_succeeded"] += succeeded
        if strategy == STRATEGY_SCRAPE:
            attempted, succeeded = refresh_scraped_items(
                scraper,
                cache,
                limit=args.limit,
                max_age_seconds=max_age_seconds,
                product_lines=lines,
                on_progress=reporter.progress,
            )
            summary["items_attempted"] += attempted
            summary["items_succeeded"] += succeeded
        if strategy == STRATEGY_AUTO:
            # HTML-scrape fallback for whatever the JSON path returned empty.
            # Previously only ever run for general_merchandise/home_and_diy;
            # it now covers every selected line, which is what unblocks
            # florist/pharmacy/pet_supply/toys/health_and_beauty.
            reporter.phase(f"scrape fallback [{', '.join(lines)}]", index=step_index, of=total_steps)
            attempted, succeeded = refresh_empty_catalog_venues(
                scraper,
                cache,
                product_lines=lines,
                limit=args.limit,
                max_age_seconds=max_age_seconds,
                on_progress=reporter.progress,
            )
            summary["items_attempted"] += attempted
            summary["items_succeeded"] += succeeded

    cache.rebuild_search_index()
    summary["counts"] = cache.counts()
    reporter.finish(summary)


def cmd_categories(args: argparse.Namespace) -> None:
    """Print the crawlable category catalog (what the admin picker shows)."""
    cache = Cache(args.db)
    client = WoltClient(policy=_policy(args))
    catalog = build_catalog(cache, discover(client, cache, force=args.refresh))
    if args.json:
        print(json.dumps([c.to_json() for c in catalog], ensure_ascii=False, indent=2))
        return
    print(f"{'key':42} {'kind':14} {'venues':>7} {'items':>7} {'pending':>8}  label")
    for c in catalog:
        print(f"{c.key:42} {c.kind:14} {c.venues:7} {c.with_items:7} {c.pending:8}  {c.label}")


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
    p.add_argument(
        "--delay",
        type=float,
        default=1.2,
        help="Baseline seconds between Wolt requests. This is a floor, not a fixed value: "
        "the crawler widens it automatically while Wolt is rate-limiting and recovers after.",
    )
    p.add_argument(
        "--max-delay", type=float, default=60.0, help="Ceiling for adaptive pacing and retry backoff (seconds)"
    )
    p.add_argument("--max-attempts", type=int, default=4, help="Tries per request before giving up (1 = no retries)")
    sub = p.add_subparsers(dest="command", required=True)

    p_crawl = sub.add_parser(
        "crawl",
        help="Crawl one or more categories end-to-end (venue discovery + items). Unbounded by default.",
    )
    p_crawl.add_argument(
        "--categories", required=True, help="Comma-separated category keys (see the 'categories' command)"
    )
    p_crawl.add_argument(
        "--phases", default="discover,items", help="Which phases to run: discover, items, or both (default: both)"
    )
    p_crawl.add_argument(
        "--limit", type=int, default=0, help="Max venues per item phase; 0 (default) means the whole category"
    )
    p_crawl.add_argument("--max-age-hours", type=float, default=DEFAULT_MENU_MAX_AGE_SECONDS / 3600)
    p_crawl.add_argument(
        "--force", action="store_true", help="Ignore freshness and re-crawl every venue in the selection"
    )
    p_crawl.add_argument(
        "--progress", choices=("text", "json"), default="text", help="Progress format; 'json' emits NDJSON events"
    )
    p_crawl.set_defaults(func=cmd_crawl)

    p_cats = sub.add_parser("categories", help="List crawlable categories discovered from Wolt's API + local cache")
    p_cats.add_argument("--refresh", action="store_true", help="Bypass the cached discovery result")
    p_cats.add_argument("--json", action="store_true")
    p_cats.set_defaults(func=cmd_categories)

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
