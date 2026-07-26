"""Local web UI: a real search page (multi-select filters, price range,
include/exclude shops, sort, honest result counts, clickable Wolt links)
plus an admin page to start/stop/monitor the crawl jobs from cli.py.

Crawl jobs run as subprocesses of this server (same `wolt-il-refresh` CLI,
invoked via `python -m wolt_il_search.cli ...`) so a page reload or browser
close doesn't kill them, and their stdout/stderr goes to a log file you can
tail from the admin page.
"""

from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from .cache import DEFAULT_DB_PATH, Cache
from .search import SearchFilters, search_full

DB_PATH = os.environ.get("WOLT_IL_DB")
JOB_LOG_DIR = (Path(DB_PATH).parent if DB_PATH else DEFAULT_DB_PATH.parent) / "job-logs"

# Sections define both admin-UI grouping and the order you'd actually run
# things in: quickstart (optional one-click bootstrap) -> setup (region/venue
# discovery, must run before anything else) -> refresh (per-venue item
# crawls, the ones you re-run periodically to pick up new/changed/removed
# items) -> maintenance (rarely needed by hand).
JOB_SPECS: dict[str, dict] = {
    "full": {
        "args": ["full", "--menu-limit", "{limit}"],
        "section": "quickstart",
        "description": "Regions + venues + a menu batch, in one shot — good for bootstrapping an empty cache",
        "has_limit": True,
        "default_limit": 200,
        "heavy": True,
    },
    "regions": {
        "args": ["regions"],
        "section": "setup",
        "description": "Refresh the 24 Israel region list",
    },
    "venues": {
        "args": ["venues"],
        "section": "setup",
        "description": "Refresh restaurant venues (all regions)",
    },
    "retail-venues": {
        "args": ["retail-venues"],
        "section": "setup",
        "description": "Refresh retail venues: electronics, general_merchandise, home_and_diy, ... (all regions)",
    },
    "grocery-venues": {
        "args": ["grocery-venues"],
        "section": "setup",
        "description": "Refresh grocery/convenience venues: Wolt Market, AM:PM, mini-markets, ... (all regions)",
    },
    "menus": {
        "args": ["menus", "--limit", "{limit}", "--max-age-hours", "{max_age_hours}"],
        "section": "refresh",
        "description": "Fetch/refresh restaurant menus (JSON path)",
        "has_limit": True,
        "has_max_age": True,
        "default_limit": 200,
        "default_max_age_hours": 168,
        "heavy": True,
    },
    "menus-retail": {
        "args": [
            "menus",
            "--limit",
            "{limit}",
            "--max-age-hours",
            "{max_age_hours}",
            "--product-lines",
            "general_merchandise,home_and_diy",
        ],
        "section": "refresh",
        "description": "Fetch/refresh general_merchandise/home_and_diy items (JSON path)",
        "has_limit": True,
        "has_max_age": True,
        "default_limit": 200,
        "default_max_age_hours": 168,
        "heavy": True,
    },
    "electronics-items": {
        "args": ["electronics-items", "--limit", "{limit}", "--max-age-hours", "{max_age_hours}"],
        "section": "refresh",
        "description": "Fetch/refresh electronics item catalogs (HTML scrape)",
        "has_limit": True,
        "has_max_age": True,
        "default_limit": 50,
        "default_max_age_hours": 168,
        "heavy": True,
    },
    "retail-rescrape": {
        "args": ["retail-rescrape", "--limit", "{limit}", "--max-age-hours", "{max_age_hours}"],
        "section": "refresh",
        "description": "Rescrape empty-catalog general_merchandise/home_and_diy venues (HTML scrape)",
        "has_limit": True,
        "has_max_age": True,
        "default_limit": 50,
        "default_max_age_hours": 168,
        "heavy": True,
    },
    "grocery-items": {
        "args": ["grocery-items", "--limit", "{limit}", "--max-age-hours", "{max_age_hours}"],
        "section": "refresh",
        "description": "Fetch/refresh grocery item catalogs (HTML scrape, same mechanism as electronics-items)",
        "has_limit": True,
        "has_max_age": True,
        "default_limit": 50,
        "default_max_age_hours": 168,
        "heavy": True,
    },
    "reindex": {
        "args": ["reindex"],
        "section": "maintenance",
        "description": (
            "Rebuild the FTS5 search index — every refresh job above already does this on "
            "completion, this is only for forcing a rebuild without running a crawl"
        ),
    },
}

SECTION_TITLES = {
    "quickstart": "Quick start",
    "setup": "1. Setup — region & venue discovery",
    "refresh": "2. Refresh items — run periodically to pick up new/changed/removed items",
    "maintenance": "3. Maintenance",
}
SECTION_ORDER = ["quickstart", "setup", "refresh", "maintenance"]


class JobManager:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def is_running(self, name: str) -> bool:
        job = self.jobs.get(name)
        return bool(job and job["proc"].poll() is None)

    def start(self, name: str, limit: int, delay: float, max_age_hours: float) -> Path:
        if name not in JOB_SPECS:
            raise ValueError(f"unknown job: {name}")
        if self.is_running(name):
            raise RuntimeError(f"job '{name}' is already running")

        JOB_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = JOB_LOG_DIR / f"{name}.log"
        args = [a.format(limit=limit, max_age_hours=max_age_hours) for a in JOB_SPECS[name]["args"]]
        cmd = [sys.executable, "-m", "wolt_il_search.cli", "--delay", str(delay)]
        if DB_PATH:
            cmd += ["--db", DB_PATH]
        cmd += args

        # Popen dup()s the fd into the child on launch, so the parent's handle
        # can close immediately — holding it open in self.jobs would leak one
        # fd per job start for the life of the webapp process.
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        self.jobs[name] = {"proc": proc, "log_path": log_path, "started_at": time.time()}
        return log_path

    def stop(self, name: str) -> bool:
        job = self.jobs.get(name)
        if job and self.is_running(name):
            proc = job["proc"]
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            return True
        return False

    def status(self, name: str) -> dict:
        job = self.jobs.get(name)
        if not job:
            return {"running": False, "started_at": None, "returncode": None}
        running = self.is_running(name)
        return {
            "running": running,
            "started_at": job["started_at"],
            "returncode": None if running else job["proc"].returncode,
        }

    def tail_log(self, name: str, lines: int = 200) -> str:
        job = self.jobs.get(name)
        if not job or not job["log_path"].exists():
            return ""
        text = job["log_path"].read_text(errors="replace")
        return "\n".join(text.splitlines()[-lines:])


job_manager = JobManager()
app = FastAPI(title="wolt-il-search")


def _cache() -> Cache:
    return Cache(DB_PATH)


def _csv(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    items = tuple(v.strip() for v in value.split(",") if v.strip())
    return items or None


@app.get("/api/search")
def api_search(
    q: str,
    regions: str | None = None,
    product_lines: str | None = None,
    open: bool = False,  # noqa: A002 - matches the query param name intentionally
    min_rating: float | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    include_venues: str | None = None,
    exclude_venues: str | None = None,
    categories: str | None = None,
    sort: str = "relevance",
    items_per_venue: int = 5,
    offset: int = 0,
    limit: int = 20,
) -> JSONResponse:
    cache = _cache()
    try:
        filters = SearchFilters(
            regions=_csv(regions),
            only_open=open,
            min_rating=min_rating,
            product_lines=_csv(product_lines),
            include_venues=_csv(include_venues),
            exclude_venues=_csv(exclude_venues),
            categories=_csv(categories),
            min_price=min_price,
            max_price=max_price,
            sort=sort,
            items_per_venue=items_per_venue,
        )
        response = search_full(cache, q, filters, offset=offset, limit=limit)
        return JSONResponse(
            {
                "results": [dataclasses.asdict(r) for r in response.results],
                "total": response.total,
                "offset": response.offset,
                "limit": response.limit,
                "venue_facets": [dataclasses.asdict(f) for f in response.venue_facets],
                "category_facets": [dataclasses.asdict(f) for f in response.category_facets],
            }
        )
    finally:
        cache.close()


@app.get("/api/regions")
def api_regions() -> JSONResponse:
    cache = _cache()
    try:
        return JSONResponse([dict(r) for r in cache.list_regions()])
    finally:
        cache.close()


@app.get("/api/product-lines")
def api_product_lines() -> JSONResponse:
    cache = _cache()
    try:
        rows = cache.conn.execute(
            "SELECT product_line, COUNT(*) c FROM venues GROUP BY product_line ORDER BY c DESC"
        ).fetchall()
        return JSONResponse([{"product_line": r["product_line"], "count": r["c"]} for r in rows])
    finally:
        cache.close()


@app.get("/api/status")
def api_status() -> JSONResponse:
    cache = _cache()
    try:
        counts = cache.counts()
        by_pl = cache.conn.execute(
            """
            SELECT product_line,
                   COUNT(*) total,
                   SUM(CASE WHEN menu_fetched_at IS NOT NULL THEN 1 ELSE 0 END) fetched
            FROM venues GROUP BY product_line ORDER BY total DESC
            """
        ).fetchall()
        counts["by_product_line"] = [dict(r) for r in by_pl]
        counts["jobs"] = {name: job_manager.status(name) for name in JOB_SPECS}
        return JSONResponse(counts)
    finally:
        cache.close()


@app.post("/api/jobs/{name}/start")
def api_job_start(name: str, limit: int = 200, delay: float = 1.5, max_age_hours: float = 168.0) -> JSONResponse:
    # Clamp rather than trust the caller: an admin API with no auth (see
    # README) shouldn't let anyone on the same network disable rate limiting
    # (delay=0), kick off an effectively-unbounded crawl, or set an age
    # cutoff so large it never re-touches an already-fetched venue.
    limit = max(1, min(limit, 5000))
    delay = max(0.5, min(delay, 30.0))
    max_age_hours = max(0.0, min(max_age_hours, 24 * 365))
    try:
        job_manager.start(name, limit=limit, delay=delay, max_age_hours=max_age_hours)
    except ValueError as e:
        raise HTTPException(404, str(e)) from e
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e

    # Remember these as this job's defaults for next time, so restarting
    # after a stop doesn't mean re-typing the same limit/max-age/delay.
    cache = _cache()
    try:
        cache.set_meta(
            f"job_params:{name}",
            json.dumps({"limit": limit, "max_age_hours": max_age_hours, "delay": delay}),
        )
    finally:
        cache.close()
    return JSONResponse({"started": True})


@app.post("/api/jobs/{name}/stop")
def api_job_stop(name: str) -> JSONResponse:
    if name not in JOB_SPECS:
        raise HTTPException(404, f"unknown job: {name}")
    stopped = job_manager.stop(name)
    return JSONResponse({"stopped": stopped})


@app.get("/api/jobs/{name}/log")
def api_job_log(name: str, lines: int = 200) -> JSONResponse:
    if name not in JOB_SPECS:
        raise HTTPException(404, f"unknown job: {name}")
    return JSONResponse({"log": job_manager.tail_log(name, lines), "status": job_manager.status(name)})


@app.get("/api/jobs")
def api_jobs() -> JSONResponse:
    return JSONResponse({name: job_manager.status(name) for name in JOB_SPECS})


@app.get("/api/jobs/meta")
def api_jobs_meta() -> JSONResponse:
    """Per-job config (section, description, which controls apply, defaults)
    — the admin page builds its whole layout from this rather than
    hardcoding it a second time in JS. Defaults are whatever this job was
    last started with, if it's ever been started, falling back to the
    hardcoded JOB_SPECS default otherwise.
    """
    cache = _cache()
    try:
        result = {}
        for name, spec in JOB_SPECS.items():
            saved_raw = cache.get_meta(f"job_params:{name}")
            saved = json.loads(saved_raw) if saved_raw else {}
            result[name] = {
                "section": spec["section"],
                "description": spec["description"],
                "has_limit": spec.get("has_limit", False),
                "has_max_age": spec.get("has_max_age", False),
                "default_limit": saved.get("limit", spec.get("default_limit", 200)),
                "default_max_age_hours": saved.get("max_age_hours", spec.get("default_max_age_hours", 168.0)),
                "default_delay": saved.get("delay", 1.5),
                "heavy": spec.get("heavy", False),
            }
        return JSONResponse(result)
    finally:
        cache.close()


@app.get("/", response_class=HTMLResponse)
def search_page() -> str:
    return SEARCH_HTML


@app.get("/admin", response_class=HTMLResponse)
def admin_page() -> str:
    return ADMIN_HTML


_BASE_CSS = r"""
:root {
  /* Read directly off wolt.com's live computed styles (getComputedStyle on
     a real venue page, --al-color-* custom properties), not guessed or
     reconstructed from a cached memory of an older palette. Wolt's current
     dark theme is a navy-tinted dark, not pure black.
     Structural chrome (topbar, sidebar, item cards) uses Wolt's secondary
     surface tone (--al-color-bg-surface-secondary), a level above the
     page's own background — not identical to it, and not the brighter
     --al-color-bg-surface either. Card/panel borders are dropped in favor
     of that subtle tone shift plus shadow for separation. */
  color-scheme: dark;
  --bg: #0a0c17;
  --surface: #161929;
  --border: #3b415e;
  --text: #e3deda;
  --text-muted: #949ab9;
  --accent: #54bce1;
  --accent-hover: #71d2f6;
  --accent-fill: #71d2f6;
  --accent-fill-hover: #54bce1;
  --text-on-accent: #010f15;
  --accent-soft: #04232e;
  --green: #65c466;
  --green-soft: #09250a;
  --red: #ff9280;
  --red-soft: #460603;
  --radius: 16px;
  --radius-sm: 10px;
  --shadow: 0 0 1px rgba(0,0,0,.5), 0 2px 6px rgba(0,0,0,.4), 0 8px 20px rgba(0,0,0,.5);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Arimo", "Open Sans", sans-serif;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font-size: 15px; line-height: 1.45; }
a { color: var(--accent); }
[dir="auto"] { unicode-bidi: plaintext; }

.topbar {
  background: var(--surface); border-bottom: 1px solid var(--border);
  padding: 0.9rem 1.5rem; display: flex; align-items: center; justify-content: space-between;
  position: sticky; top: 0; z-index: 10;
}
.topbar .brand { font-weight: 800; font-size: 1.1rem; letter-spacing: -0.01em; }
.topbar .brand .dot { color: var(--accent); }
.topbar nav a { font-size: 0.85rem; font-weight: 600; text-decoration: none; margin-left: 1rem; color: var(--text-muted); }
.topbar nav a:hover { color: var(--accent); }
.topbar .stats { font-size: 0.8rem; color: var(--text-muted); }

.layout { max-width: 1200px; margin: 0 auto; padding: 1.5rem; display: grid; grid-template-columns: 280px 1fr; gap: 1.5rem; }
@media (max-width: 860px) {
  .layout { grid-template-columns: 1fr; }
  #filtersCol { order: 2; }
  #mainCol { order: 1; }
}

.panel { background: var(--surface); border-radius: var(--radius); box-shadow: var(--shadow); }
.panel + .panel { margin-top: 1rem; }
.panel-header {
  display: block; padding: 0.8rem 1rem; border-bottom: 1px solid var(--border); font-weight: 600;
  font-size: 0.85rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em; cursor: pointer;
}
.panel-header:focus-visible { outline: 2px solid var(--accent); outline-offset: -2px; }
details:not([open]) > .panel-header { border-bottom: none; }
.panel-body { padding: 1rem; }
.loading-state { text-align: center; padding: 3rem 1rem; color: var(--text-muted); }
.loading-state::after {
  content: ''; display: inline-block; width: 0.9rem; height: 0.9rem; margin-left: 0.5rem;
  border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%;
  animation: spin 0.7s linear infinite; vertical-align: middle;
}
@keyframes spin { to { transform: rotate(360deg); } }

.field { margin-bottom: 1rem; }
.field:last-child { margin-bottom: 0; }
.field label { display: block; font-size: 0.8rem; font-weight: 600; color: var(--text-muted); margin-bottom: 0.35rem; }
.field input[type="text"], .field input[type="number"], .field select {
  width: 100%; padding: 0.5rem 0.6rem; border: 1px solid var(--border); border-radius: var(--radius-sm);
  font-size: 0.9rem; color: var(--text); background: var(--surface);
}
.field input:focus, .field select:focus { outline: 2px solid var(--accent); border-color: var(--accent); }
.range-row { display: flex; gap: 0.5rem; }
.checklist { max-height: 220px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.3rem; }
.checklist label { display: flex; align-items: center; gap: 0.45rem; font-size: 0.85rem; font-weight: 400; color: var(--text); cursor: pointer; }
.checklist .count { color: var(--text-muted); font-size: 0.78rem; }
.chk-toggle {
  font-size: 0.75rem; color: var(--accent); cursor: pointer; text-decoration: underline; margin-bottom: 0.4rem;
  display: inline-block; background: none; border: none; padding: 0; font-family: inherit;
}
.chk-toggle:focus-visible { outline: 2px solid var(--accent); border-radius: 2px; }
.facet-hint { font-weight: 400; text-transform: none; letter-spacing: 0; font-size: 0.72rem; color: var(--text-muted); display: block; margin-top: 0.15rem; }
.facet-row {
  display: flex; align-items: center; gap: 0.5rem; padding: 0.2rem 0; cursor: pointer; font-size: 0.85rem;
  background: none; border: none; width: 100%; text-align: left; font-family: inherit; color: var(--text);
}
.facet-row:hover .facet-label { color: var(--accent); }
.facet-row:focus-visible { outline: 2px solid var(--accent); border-radius: 4px; }
.facet-icon {
  width: 1rem; height: 1rem; border: 1.5px solid var(--border); border-radius: 4px; flex-shrink: 0;
  display: inline-flex; align-items: center; justify-content: center; font-size: 0.65rem; font-weight: 700; color: transparent;
}
.facet-icon.include { background: var(--green-soft); border-color: var(--green); color: var(--green); }
.facet-icon.exclude { background: var(--red-soft); border-color: var(--red); color: var(--red); }
.facet-label { flex: 1; }
.facet-count { color: var(--text-muted); font-size: 0.78rem; }

.searchbar { display: flex; gap: 0.6rem; margin-bottom: 1rem; }
.search-input-wrap { position: relative; flex: 1; }
.search-icon {
  position: absolute; left: 0.95rem; top: 50%; transform: translateY(-50%);
  width: 1.1rem; height: 1.1rem; color: var(--text-muted); pointer-events: none;
}
.searchbar input[type="text"] {
  width: 100%; padding: 0.75rem 1.1rem 0.75rem 2.6rem; font-size: 1rem; border: none;
  border-radius: var(--radius); background: var(--surface); color: var(--text);
}
.searchbar input[type="text"]:focus { outline: 2px solid var(--accent); }
button.primary {
  background: var(--accent-fill); color: var(--text-on-accent); border: none; border-radius: 999px;
  padding: 0.75rem 1.5rem; font-size: 0.95rem; font-weight: 700; cursor: pointer;
}
button.primary:hover { background: var(--accent-fill-hover); }

.result-meta { color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.9rem; }

.venue-card {
  background: var(--surface); border-radius: var(--radius);
  box-shadow: var(--shadow); padding: 0.9rem 1.1rem; margin-bottom: 0.8rem;
}
.venue-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 0.6rem; }
.venue-name { font-weight: 700; font-size: 1rem; }
.venue-name a { color: var(--text); text-decoration: none; }
.venue-name a:hover { color: var(--accent); }
.badge { font-size: 0.72rem; font-weight: 700; padding: 0.15rem 0.55rem; border-radius: 999px; white-space: nowrap; letter-spacing: 0.02em; }
.badge.open { background: var(--green-soft); color: var(--green); }
.badge.closed { background: var(--red-soft); color: var(--red); }
.venue-sub { color: var(--text-muted); font-size: 0.82rem; margin-top: 0.15rem; }
.venue-sub .sep { margin: 0 0.35rem; opacity: 0.5; }
.tag-pill { display: inline-block; background: var(--accent-soft); color: var(--accent); border-radius: 999px; padding: 0.05rem 0.5rem; font-size: 0.72rem; margin-top: 0.4rem; margin-right: 0.3rem; }

.items { margin-top: 0.6rem; border-top: 1px solid var(--border); padding-top: 0.5rem; }
.item-row { display: flex; justify-content: space-between; align-items: baseline; gap: 0.6rem; padding: 0.22rem 0; font-size: 0.88rem; }
.item-row a { color: var(--text); text-decoration: none; }
.item-row a:hover { color: var(--accent); text-decoration: underline; }
.item-price { color: var(--text-muted); white-space: nowrap; font-variant-numeric: tabular-nums; }
.more-items {
  font-size: 0.8rem; color: var(--accent); cursor: pointer; margin-top: 0.3rem; display: inline-block;
  background: none; border: none; padding: 0; font-family: inherit;
}
.more-items:focus-visible { outline: 2px solid var(--accent); border-radius: 2px; }
.extra-items { display: none; }
.extra-items.shown { display: block; }
.view-full-menu { display: block; margin-top: 0.4rem; font-size: 0.85rem; }

.empty-state { text-align: center; padding: 3rem 1rem; color: var(--text-muted); }
.load-more { display: block; margin: 1rem auto; }
"""


SEARCH_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Wolt IL Search</title>
<style>""" + _BASE_CSS + r"""
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">Wolt IL <span class="dot">Search</span></div>
  <div style="display:flex; align-items:center; gap:1rem;">
    <span class="stats" id="headerStats"></span>
    <nav><a href="/admin">Admin &rarr;</a></nav>
  </div>
</div>

<div class="layout">
  <div id="filtersCol">
    <div class="panel">
      <details open>
        <summary class="panel-header">Regions</summary>
        <div class="panel-body">
          <button type="button" class="chk-toggle" data-target="regionList">clear</button>
          <div class="checklist" id="regionList"></div>
        </div>
      </details>
    </div>
    <div class="panel">
      <details open>
        <summary class="panel-header">Categories</summary>
        <div class="panel-body">
          <button type="button" class="chk-toggle" data-target="plList">clear</button>
          <div class="checklist" id="plList"></div>
        </div>
      </details>
    </div>
    <div class="panel">
      <details open>
        <summary class="panel-header">More filters</summary>
        <div class="panel-body">
          <div class="field">
            <label><input type="checkbox" id="onlyOpen"> Open now</label>
          </div>
          <div class="field">
            <label for="minRating">Min rating</label>
            <input type="number" id="minRating" min="0" max="10" step="0.5" placeholder="e.g. 8">
          </div>
          <div class="field">
            <label>Price range (ILS)</label>
            <div class="range-row">
              <input type="number" id="minPrice" placeholder="e.g. 20">
              <input type="number" id="maxPrice" placeholder="e.g. 100">
            </div>
          </div>
          <div class="field">
            <label for="sort">Sort by</label>
            <select id="sort">
              <option value="relevance">Relevance</option>
              <option value="rating">Rating</option>
              <option value="price_asc">Price: low to high</option>
              <option value="price_desc">Price: high to low</option>
            </select>
          </div>
        </div>
      </details>
    </div>
    <div class="panel" id="shopFacetPanel" style="display:none;">
      <details open>
        <summary class="panel-header">Shops in these results <span class="facet-hint">click: include &middot; again: exclude &middot; again: clear</span></summary>
        <div class="panel-body">
          <button type="button" class="chk-toggle" id="clearShopFacets">clear</button>
          <div class="checklist" id="shopFacetList"></div>
        </div>
      </details>
    </div>
    <div class="panel" id="categoryFacetPanel" style="display:none;">
      <details open>
        <summary class="panel-header">Item categories in these results</summary>
        <div class="panel-body">
          <button type="button" class="chk-toggle" id="clearCategoryFacets">clear</button>
          <div class="checklist" id="categoryFacetList"></div>
        </div>
      </details>
    </div>
  </div>

  <div id="mainCol">
    <div class="searchbar">
      <div class="search-input-wrap">
        <svg class="search-icon" viewBox="0 0 20 20" aria-hidden="true"><circle cx="9" cy="9" r="6" fill="none" stroke="currentColor" stroke-width="1.8"/><line x1="13.2" y1="13.2" x2="17.5" y2="17.5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>
        <input type="text" id="q" placeholder="Search venues and items nationwide..." autofocus>
      </div>
      <button class="primary" id="go">Search</button>
    </div>
    <div class="result-meta" id="resultMeta"></div>
    <div id="results"></div>
    <button class="primary load-more" id="loadMore" style="display:none; background:var(--surface); color:var(--accent); border:1px solid var(--border);">Load more</button>
  </div>
</div>

<script>
const PAGE_SIZE = 20;
let currentOffset = 0;
let currentTotal = 0;
// slug -> 'include' | 'exclude', persists across filter tweaks, cleared on a fresh text search
let shopFacetState = new Map();
// selected item-category facet values
let selectedCategories = new Set();

function esc(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

const PL_LABELS = {
  restaurant: 'Restaurants', electronics: 'Electronics', general_merchandise: 'General Merchandise',
  home_and_diy: 'Home & DIY', florist: 'Florist', pet_supply: 'Pet Supply',
  toys_games_and_kids: 'Toys & Kids', pharmacy: 'Pharmacy', health_and_beauty: 'Health & Beauty',
  grocery: 'Grocery',
};
function humanizeLabel(v) { return PL_LABELS[v] || v; }

function checklistHtml(id, items, valueKey, labelKey, countKey, humanize) {
  const el = document.getElementById(id);
  el.innerHTML = items.map(it => `
    <label><input type="checkbox" value="${esc(it[valueKey])}" data-group="${id}">
      ${esc(humanize ? humanizeLabel(it[labelKey]) : it[labelKey])} ${countKey ? `<span class="count">(${it[countKey]})</span>` : ''}
    </label>`).join('');
}

async function loadFilters() {
  const regions = await (await fetch('/api/regions')).json();
  checklistHtml('regionList', regions, 'slug', 'name', null, false);
  const plines = await (await fetch('/api/product-lines')).json();
  checklistHtml('plList', plines, 'product_line', 'product_line', 'count', true);
  const status = await (await fetch('/api/status')).json();
  document.getElementById('headerStats').textContent =
    `${status.venues.toLocaleString()} venues · ${status.menu_items.toLocaleString()} items indexed`;
}

document.querySelectorAll('.chk-toggle').forEach(t => {
  t.addEventListener('click', () => {
    if (t.dataset.target) document.querySelectorAll(`input[data-group="${t.dataset.target}"]`).forEach(cb => cb.checked = false);
    doSearch();
  });
});

function checkedValues(groupId) {
  return Array.from(document.querySelectorAll(`#${groupId} input:checked`)).map(cb => cb.value);
}

function fmtPrice(cents) {
  return cents == null ? '' : (cents / 100).toFixed(2) + ' ILS';
}

const ITEMS_PER_VENUE_FETCHED = 20;
const ITEMS_SHOWN_BY_DEFAULT = 5;

function itemRowHtml(it) {
  return `<div class="item-row">
    <a href="${it.wolt_url}" target="_blank" rel="noopener" dir="auto">${esc(it.name)}</a>
    <span class="item-price">${fmtPrice(it.price)}</span>
  </div>`;
}

function venueCardHtml(v) {
  const shown = v.matched_items.slice(0, ITEMS_SHOWN_BY_DEFAULT);
  const hidden = v.matched_items.slice(ITEMS_SHOWN_BY_DEFAULT);
  const beyondFetched = v.total_matched_items - v.matched_items.length;
  const moreCount = hidden.length + beyondFetched;
  return `
    <div class="venue-card">
      <div class="venue-head">
        <div>
          <div class="venue-name" dir="auto"><a href="${v.wolt_url}" target="_blank" rel="noopener">${esc(v.name)}</a></div>
          <div class="venue-sub" dir="auto">${esc(v.region_slug)}<span class="sep">&middot;</span>rating ${v.rating ?? '?'}<span class="sep">&middot;</span>${esc(v.address ?? '')}</div>
          <span class="tag-pill">${esc(humanizeLabel(v.product_line ?? ''))}</span>
        </div>
        <span class="badge ${v.online ? 'open' : 'closed'}">${v.online ? 'OPEN' : 'CLOSED'}</span>
      </div>
      ${shown.length ? `<div class="items">${shown.map(itemRowHtml).join('')}
        ${hidden.length ? `<div class="extra-items">${hidden.map(itemRowHtml).join('')}</div>` : ''}
        ${moreCount > 0 ? `<button type="button" class="more-items" data-more-count="${moreCount}" data-hidden="${hidden.length}">+${moreCount} more item${moreCount === 1 ? '' : 's'} at this shop</button>` : ''}
        ${beyondFetched > 0 ? `<a class="view-full-menu" style="display:none" href="${v.wolt_url}" target="_blank" rel="noopener">View full menu on Wolt &rarr;</a>` : ''}
      </div>` : ''}
    </div>`;
}

document.getElementById('results').addEventListener('click', (e) => {
  const btn = e.target.closest('.more-items');
  if (!btn) return;
  const itemsDiv = btn.closest('.items');
  const extra = itemsDiv.querySelector('.extra-items');
  const fullMenuLink = itemsDiv.querySelector('.view-full-menu');
  const opening = !btn.classList.contains('is-open');
  btn.classList.toggle('is-open', opening);
  if (extra) extra.classList.toggle('shown', opening);
  if (fullMenuLink) fullMenuLink.style.display = opening ? 'block' : 'none';
  const hiddenCount = parseInt(btn.dataset.hidden, 10);
  const moreCount = parseInt(btn.dataset.moreCount, 10);
  btn.textContent = opening
    ? (hiddenCount > 0 ? 'show fewer items' : 'show fewer items — see full menu above')
    : `+${moreCount} more item${moreCount === 1 ? '' : 's'} at this shop`;
});

function renderShopFacets(facets) {
  const panel = document.getElementById('shopFacetPanel');
  const list = document.getElementById('shopFacetList');
  if (!facets.length) { panel.style.display = 'none'; return; }
  panel.style.display = 'block';
  list.innerHTML = facets.map(f => {
    const state = shopFacetState.get(f.value);
    const cls = state === 'include' ? 'include' : state === 'exclude' ? 'exclude' : '';
    const icon = state === 'include' ? '&#10003;' : state === 'exclude' ? '&#10005;' : '';
    const stateLabel = state === 'include' ? ' (included)' : state === 'exclude' ? ' (excluded)' : '';
    return `<button type="button" class="facet-row" data-slug="${esc(f.value)}" aria-label="${esc(f.label)}${stateLabel}">
      <span class="facet-icon ${cls}" aria-hidden="true">${icon}</span>
      <span class="facet-label" dir="auto">${esc(f.label)}</span>
      <span class="facet-count">${f.count}</span>
    </button>`;
  }).join('');
}

function renderCategoryFacets(facets) {
  const panel = document.getElementById('categoryFacetPanel');
  const list = document.getElementById('categoryFacetList');
  if (!facets.length) { panel.style.display = 'none'; return; }
  panel.style.display = 'block';
  list.innerHTML = facets.map(f => {
    const checked = selectedCategories.has(f.value) ? 'checked' : '';
    return `<label><input type="checkbox" value="${esc(f.value)}" ${checked} data-category-facet="1">
      <span dir="auto">${esc(f.label)}</span> <span class="count">(${f.count})</span>
    </label>`;
  }).join('');
}

document.getElementById('shopFacetList').addEventListener('click', (e) => {
  const row = e.target.closest('.facet-row');
  if (!row) return;
  const slug = row.dataset.slug;
  const cur = shopFacetState.get(slug);
  if (!cur) shopFacetState.set(slug, 'include');
  else if (cur === 'include') shopFacetState.set(slug, 'exclude');
  else shopFacetState.delete(slug);
  doSearch();
});

document.getElementById('categoryFacetList').addEventListener('change', (e) => {
  if (!e.target.matches('input[data-category-facet]')) return;
  if (e.target.checked) selectedCategories.add(e.target.value);
  else selectedCategories.delete(e.target.value);
  doSearch();
});

document.getElementById('clearShopFacets').addEventListener('click', () => { shopFacetState.clear(); doSearch(); });
document.getElementById('clearCategoryFacets').addEventListener('click', () => { selectedCategories.clear(); doSearch(); });

function buildParams(offset) {
  const params = new URLSearchParams({
    q: document.getElementById('q').value.trim(), offset, limit: PAGE_SIZE, items_per_venue: ITEMS_PER_VENUE_FETCHED,
  });
  const regions = checkedValues('regionList');
  const pls = checkedValues('plList');
  if (regions.length) params.set('regions', regions.join(','));
  if (pls.length) params.set('product_lines', pls.join(','));
  if (document.getElementById('onlyOpen').checked) params.set('open', 'true');
  const minRating = document.getElementById('minRating').value;
  if (minRating) params.set('min_rating', minRating);
  const minPrice = document.getElementById('minPrice').value;
  const maxPrice = document.getElementById('maxPrice').value;
  if (minPrice) params.set('min_price', minPrice);
  if (maxPrice) params.set('max_price', maxPrice);
  const includeSlugs = [...shopFacetState.entries()].filter(([, v]) => v === 'include').map(([k]) => k);
  const excludeSlugs = [...shopFacetState.entries()].filter(([, v]) => v === 'exclude').map(([k]) => k);
  if (includeSlugs.length) params.set('include_venues', includeSlugs.join(','));
  if (excludeSlugs.length) params.set('exclude_venues', excludeSlugs.join(','));
  if (selectedCategories.size) params.set('categories', [...selectedCategories].join(','));
  params.set('sort', document.getElementById('sort').value);
  return params;
}

async function doSearch(opts) {
  if (opts && opts.resetFacetState) { shopFacetState.clear(); selectedCategories.clear(); }
  const q = document.getElementById('q').value.trim();
  const results = document.getElementById('results');
  const meta = document.getElementById('resultMeta');
  const loadMore = document.getElementById('loadMore');
  currentOffset = 0;
  if (!q) {
    results.innerHTML = ''; meta.textContent = ''; loadMore.style.display = 'none';
    renderShopFacets([]); renderCategoryFacets([]);
    return;
  }

  results.innerHTML = '<div class="loading-state">Searching...</div>';
  meta.textContent = '';
  loadMore.style.display = 'none';

  const data = await (await fetch('/api/search?' + buildParams(0))).json();
  currentTotal = data.total;
  currentOffset = data.results.length;
  renderShopFacets(data.venue_facets);
  renderCategoryFacets(data.category_facets);

  if (data.results.length === 0) {
    results.innerHTML = '<div class="empty-state">No results.</div>';
    return;
  }
  meta.textContent = `Showing ${data.results.length} of ${data.total.toLocaleString()} matching venues`;
  results.innerHTML = data.results.map(venueCardHtml).join('');
  loadMore.style.display = currentOffset < currentTotal ? 'block' : 'none';
}

async function loadMoreResults() {
  const btn = document.getElementById('loadMore');
  btn.disabled = true;
  btn.textContent = 'Loading...';
  try {
    const data = await (await fetch('/api/search?' + buildParams(currentOffset))).json();
    const results = document.getElementById('results');
    results.insertAdjacentHTML('beforeend', data.results.map(venueCardHtml).join(''));
    currentOffset += data.results.length;
    document.getElementById('resultMeta').textContent = `Showing ${currentOffset} of ${currentTotal.toLocaleString()} matching venues`;
    btn.style.display = currentOffset < currentTotal ? 'block' : 'none';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Load more';
  }
}

function debounce(fn, ms) {
  let handle;
  return (...args) => { clearTimeout(handle); handle = setTimeout(() => fn(...args), ms); };
}
const debouncedSearch = debounce(() => doSearch(), 400);

document.getElementById('go').addEventListener('click', () => doSearch({ resetFacetState: true }));
document.getElementById('loadMore').addEventListener('click', loadMoreResults);
document.getElementById('q').addEventListener('keydown', e => { if (e.key === 'Enter') doSearch({ resetFacetState: true }); });
['regionList', 'plList'].forEach(id => document.getElementById(id).addEventListener('change', () => doSearch()));
['onlyOpen', 'sort'].forEach(id => document.getElementById(id).addEventListener('change', () => doSearch()));
['minRating', 'minPrice', 'maxPrice'].forEach(id => document.getElementById(id).addEventListener('input', debouncedSearch));

loadFilters();
</script>
</body>
</html>
"""

ADMIN_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Wolt IL Search &mdash; Admin</title>
<style>""" + _BASE_CSS + r"""
  .layout { grid-template-columns: 1fr; max-width: 960px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.55rem 0.7rem; border-bottom: 1px solid var(--border); font-size: 0.88rem; }
  th { color: var(--text-muted); font-weight: 600; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; }
  button.start { background: var(--green); color: white; border: none; border-radius: 999px; padding: 0.4rem 0.9rem; cursor: pointer; font-weight: 700; white-space: nowrap; }
  button.stop { background: var(--red); color: white; border: none; border-radius: 999px; padding: 0.4rem 0.9rem; cursor: pointer; font-weight: 700; white-space: nowrap; }
  .dot-status { display: inline-block; width: 0.55rem; height: 0.55rem; border-radius: 50%; margin-right: 0.4rem; }
  .dot-status.running { background: var(--green); }
  .dot-status.idle { background: #b0b3b8; }
  .dot-status.done { background: var(--accent); }
  .dot-status.failed { background: var(--red); }
  pre { background: #0d1117; color: #d1d5db; padding: 0.7rem; border-radius: 8px; max-height: 260px; overflow: auto; font-size: 0.78rem; display: none; margin: 0.5rem 0 0.8rem 0; }
  pre.shown { display: block; }
  .toggle-log { font-size: 0.78rem; cursor: pointer; color: var(--accent); background: none; border: none; padding: 0; font-family: inherit; }
  .toggle-log:focus-visible { outline: 2px solid var(--accent); border-radius: 2px; }
  button.start:disabled, button.stop:disabled, .load-more:disabled { opacity: 0.6; cursor: not-allowed; }
  .job-desc { color: var(--text-muted); font-size: 0.78rem; margin-top: 0.15rem; }

  .job-section-title { font-weight: 700; font-size: 0.9rem; margin: 1.5rem 0 0.4rem; }
  .job-section-title:first-child { margin-top: 0; }
  .job-row-flex {
    display: flex; flex-wrap: wrap; align-items: flex-end; gap: 1rem;
    padding: 0.9rem 0; border-bottom: 1px solid var(--border);
  }
  .job-row-flex:last-of-type { border-bottom: none; }
  .job-info { flex: 1 1 220px; min-width: 200px; }
  .job-controls-row { display: flex; gap: 0.6rem; flex-wrap: wrap; }
  .job-controls-row .field { margin-bottom: 0; }
  .job-controls-row .field label { margin-bottom: 0.2rem; white-space: nowrap; }
  .job-controls-row .field input { width: 5.5rem; padding: 0.35rem 0.4rem; font-size: 0.85rem; }
  .job-status-row { flex: 0 1 170px; min-width: 150px; font-size: 0.85rem; }
  .job-actions-row { display: flex; align-items: center; }
  .info-icon {
    display: inline-flex; align-items: center; justify-content: center;
    width: 0.95rem; height: 0.95rem; border-radius: 50%; position: relative;
    background: var(--border); color: var(--text-muted); font-size: 0.65rem; font-weight: 700;
    cursor: help; margin-left: 0.15rem; flex-shrink: 0; vertical-align: middle;
  }
  .info-icon:hover::after, .info-icon:focus-visible::after {
    content: attr(data-tip); position: absolute; bottom: 135%; left: 50%; transform: translateX(-50%);
    background: var(--text); color: var(--bg); padding: 0.4rem 0.6rem; border-radius: 6px;
    font-size: 0.72rem; font-weight: 400; white-space: normal; width: 14rem;
    box-shadow: var(--shadow); z-index: 20; pointer-events: none;
  }
  .info-icon:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">Wolt IL <span class="dot">Search</span></div>
  <div style="display:flex; align-items:center; gap:1rem;">
    <nav><a href="/">&larr; Search</a></nav>
  </div>
</div>
<div class="layout">
  <div class="panel">
    <div class="panel-header">Cache status</div>
    <div class="panel-body"><table id="statusTable"></table></div>
  </div>
  <div class="panel">
    <div class="panel-header">By category</div>
    <div class="panel-body">
      <table><thead><tr><th>product_line</th><th>venues</th><th>with items</th></tr></thead><tbody id="plBody"></tbody></table>
    </div>
  </div>
  <div class="panel">
    <div class="panel-header">Crawl jobs</div>
    <div class="panel-body" id="jobSections"></div>
  </div>
</div>

<script>
const SECTION_TITLES = {
  quickstart: 'Quick start',
  setup: '1. Setup — region &amp; venue discovery',
  refresh: '2. Refresh items — run periodically to pick up new/changed/removed items',
  maintenance: '3. Maintenance',
};
const SECTION_ORDER = ['quickstart', 'setup', 'refresh', 'maintenance'];

let jobsMeta = {};  // name -> {section, description, has_limit, has_max_age, default_limit, default_max_age_hours, heavy}, loaded once

function fmtStarted(ts) { return ts ? new Date(ts * 1000).toLocaleTimeString() : ''; }

const FIELD_TIPS = {
  limit: 'Max number of venues to process in this one run.',
  maxage: 'Only re-fetch venues whose data is older than this many hours. Lower catches price/availability changes and removed items sooner; higher (or very large) skips anything fetched recently.',
  delay: 'Seconds to wait between requests to Wolt, so this crawl doesn’t hammer their servers.',
};
function infoIcon(key) {
  return `<span class="info-icon" tabindex="0" data-tip="${FIELD_TIPS[key]}">ⓘ</span>`;
}

// The status poll rebuilds every job row's HTML from scratch every 5s, which
// would otherwise silently overwrite anything you'd typed into these inputs
// before hitting Start. Read whatever's currently on screen first, and only
// fall back to the server-remembered default for a row's very first render.
function currentInputValue(id, fallback) {
  const el = document.getElementById(id);
  return el ? el.value : fallback;
}

function jobRowHtml(name, meta, s) {
  const dotClass = s.running ? 'running' : (s.returncode == null ? 'idle' : (s.returncode === 0 ? 'done' : 'failed'));
  const label = s.running ? `running since ${fmtStarted(s.started_at)}`
    : (s.returncode != null ? `exited (code ${s.returncode})` : 'idle');
  const logShown = document.getElementById(`log-${name}`)?.classList.contains('shown');

  const limitVal = currentInputValue(`limit-${name}`, meta.default_limit);
  const maxAgeVal = currentInputValue(`maxage-${name}`, meta.default_max_age_hours);
  const delayVal = currentInputValue(`delay-${name}`, meta.default_delay);

  const controls = [];
  if (meta.has_limit) {
    controls.push(`<div class="field"><label for="limit-${name}">Limit${infoIcon('limit')}</label><input type="number" min="1" id="limit-${name}" value="${limitVal}"></div>`);
  }
  if (meta.has_max_age) {
    controls.push(`<div class="field"><label for="maxage-${name}">Max age (h)${infoIcon('maxage')}</label><input type="number" min="0" id="maxage-${name}" value="${maxAgeVal}"></div>`);
  }
  controls.push(`<div class="field"><label for="delay-${name}">Delay (s)${infoIcon('delay')}</label><input type="number" min="0.5" step="0.1" id="delay-${name}" value="${delayVal}"></div>`);

  const html = `
    <div class="job-row-flex" data-job="${name}">
      <div class="job-info"><strong>${name}</strong><div class="job-desc">${meta.description}</div></div>
      <div class="job-controls-row">${controls.join('')}</div>
      <div class="job-status-row">
        <span class="dot-status ${dotClass}"></span>${label}<br>
        <button type="button" class="toggle-log" id="toggle-log-${name}" onclick="toggleLog('${name}')">${logShown ? 'hide log' : 'show log'}</button>
      </div>
      <div class="job-actions-row">${s.running
        ? `<button class="stop" onclick="stopJob('${name}')">Stop</button>`
        : `<button class="start" onclick="startJob('${name}')">Start</button>`}</div>
    </div>
    <pre id="log-${name}" class="${logShown ? 'shown' : ''}"></pre>
  `;
  return { html, logShown };
}

async function loadJobsMeta() {
  jobsMeta = await (await fetch('/api/jobs/meta')).json();
}

async function refreshStatus() {
  const status = await (await fetch('/api/status')).json();
  document.getElementById('statusTable').innerHTML = `
    <tr><th>Regions</th><td>${status.regions}</td></tr>
    <tr><th>Venues</th><td>${status.venues.toLocaleString()}</td></tr>
    <tr><th>Venues with items</th><td>${status.venues_with_menu.toLocaleString()}</td></tr>
    <tr><th>Menu items</th><td>${status.menu_items.toLocaleString()}</td></tr>
  `;
  document.getElementById('plBody').innerHTML = status.by_product_line.map(p => `
    <tr><td>${p.product_line ?? '(none)'}</td><td>${p.total.toLocaleString()}</td><td>${p.fetched.toLocaleString()}</td></tr>
  `).join('');

  const bySection = {};
  SECTION_ORDER.forEach(sec => { bySection[sec] = []; });
  Object.keys(jobsMeta).forEach(name => { bySection[jobsMeta[name].section]?.push(name); });

  const shownLogs = [];
  document.getElementById('jobSections').innerHTML = SECTION_ORDER
    .filter(sec => bySection[sec].length)
    .map(sec => {
      const rows = bySection[sec].map(name => {
        const { html, logShown } = jobRowHtml(name, jobsMeta[name], status.jobs[name]);
        if (logShown) shownLogs.push(name);
        return html;
      }).join('');
      return `<div class="job-section-title">${SECTION_TITLES[sec]}</div>${rows}`;
    }).join('');

  // The <pre> above is rebuilt empty every poll — refill any pane that was open.
  shownLogs.forEach(async (name) => {
    const data = await (await fetch(`/api/jobs/${name}/log?lines=100`)).json();
    const pre = document.getElementById(`log-${name}`);
    if (pre) pre.textContent = data.log || '(no output yet)';
  });
}

async function startJob(name) {
  const meta = jobsMeta[name];
  if (meta.heavy && !confirm(`Start "${name}"? This hits Wolt for many venues and can run for several minutes.`)) {
    return;
  }
  const params = new URLSearchParams();
  const limitInput = document.getElementById(`limit-${name}`);
  const maxAgeInput = document.getElementById(`maxage-${name}`);
  const delayInput = document.getElementById(`delay-${name}`);
  if (limitInput) params.set('limit', limitInput.value);
  if (maxAgeInput) params.set('max_age_hours', maxAgeInput.value);
  if (delayInput) params.set('delay', delayInput.value);

  const btn = document.querySelector(`.job-row-flex[data-job="${name}"] button.start`);
  if (btn) { btn.disabled = true; btn.textContent = 'Starting...'; }
  await fetch(`/api/jobs/${name}/start?${params}`, { method: 'POST' });
  refreshStatus();
}

async function stopJob(name) {
  await fetch(`/api/jobs/${name}/stop`, { method: 'POST' });
  refreshStatus();
}

async function toggleLog(name) {
  const pre = document.getElementById(`log-${name}`);
  const btn = document.getElementById(`toggle-log-${name}`);
  if (pre.classList.contains('shown')) {
    pre.classList.remove('shown');
    if (btn) btn.textContent = 'show log';
    return;
  }
  const data = await (await fetch(`/api/jobs/${name}/log?lines=100`)).json();
  pre.textContent = data.log || '(no output yet)';
  pre.classList.add('shown');
  if (btn) btn.textContent = 'hide log';
}

(async function init() {
  await loadJobsMeta();
  await refreshStatus();
  setInterval(refreshStatus, 5000);
})();
</script>
</body>
</html>
"""


def main() -> None:
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(prog="wolt-il-webui")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
