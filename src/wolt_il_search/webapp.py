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

JOB_SPECS: dict[str, list[str]] = {
    "regions": ["regions"],
    "venues": ["venues"],
    "retail-venues": ["retail-venues"],
    "menus": ["menus", "--limit", "{limit}", "--max-age-hours", "999999"],
    "menus-retail": [
        "menus",
        "--limit",
        "{limit}",
        "--max-age-hours",
        "999999",
        "--product-lines",
        "general_merchandise,home_and_diy",
    ],
    "electronics-items": ["electronics-items", "--limit", "{limit}", "--max-age-hours", "999999"],
    "retail-rescrape": ["retail-rescrape", "--limit", "{limit}"],
    "reindex": ["reindex"],
    "full": ["full", "--menu-limit", "{limit}"],
}


class JobManager:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def is_running(self, name: str) -> bool:
        job = self.jobs.get(name)
        return bool(job and job["proc"].poll() is None)

    def start(self, name: str, limit: int, delay: float) -> Path:
        if name not in JOB_SPECS:
            raise ValueError(f"unknown job: {name}")
        if self.is_running(name):
            raise RuntimeError(f"job '{name}' is already running")

        JOB_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = JOB_LOG_DIR / f"{name}.log"
        args = [a.format(limit=limit) for a in JOB_SPECS[name]]
        cmd = [sys.executable, "-m", "wolt_il_search.cli", "--delay", str(delay)]
        if DB_PATH:
            cmd += ["--db", DB_PATH]
        cmd += args

        log_file = open(log_path, "w")
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        self.jobs[name] = {"proc": proc, "log_file": log_file, "log_path": log_path, "started_at": time.time()}
        return log_path

    def stop(self, name: str) -> bool:
        job = self.jobs.get(name)
        if job and self.is_running(name):
            job["proc"].terminate()
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
def api_job_start(name: str, limit: int = 200, delay: float = 1.5) -> JSONResponse:
    try:
        job_manager.start(name, limit=limit, delay=delay)
    except ValueError as e:
        raise HTTPException(404, str(e)) from e
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
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


@app.get("/", response_class=HTMLResponse)
def search_page() -> str:
    return SEARCH_HTML


@app.get("/admin", response_class=HTMLResponse)
def admin_page() -> str:
    return ADMIN_HTML


_BASE_CSS = r"""
:root {
  /* Palette pulled from wolt.com's own production CSS (static/themes/al/default.css):
     --al-color-text-brand #009de0, --al-color-text #202125, --al-color-border #e4e4e5,
     --al-color-bg-surface-secondary #f6f6f6, --al-color-text-positive #1fc70a,
     --al-color-text-negative #f93a25, shadow recipe from --al-shadow-small. */
  color-scheme: light;
  --bg: #f6f6f6;
  --surface: #ffffff;
  --border: #e4e4e5;
  --text: #202125;
  --text-muted: #5c5d63;
  --accent: #009de0;
  --accent-hover: #0086bf;
  --accent-soft: #ebf7fd;
  --green: #1fc70a;
  --green-soft: #eafcea;
  --red: #f93a25;
  --red-soft: #fdeceb;
  --radius: 16px;
  --radius-sm: 10px;
  --shadow: 0 0 1px rgba(0,0,0,.08), 0 1px 2px rgba(0,0,0,.06), 0 2px 8px rgba(0,0,0,.08);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Arimo", "Open Sans", sans-serif;
}
:root[data-theme="dark"] {
  /* Wolt's dark theme runs pure black, not dark-gray. */
  color-scheme: dark;
  --bg: #000000;
  --surface: #171719;
  --border: #2c2c2e;
  --text: #f2f2f2;
  --text-muted: #9a9a9e;
  --accent: #1fb8f0;
  --accent-hover: #4fc6f2;
  --accent-soft: #001924;
  --green: #3ddc25;
  --green-soft: #06210a;
  --red: #ff5c47;
  --red-soft: #2a0e0b;
  --shadow: 0 0 1px rgba(0,0,0,.5), 0 2px 6px rgba(0,0,0,.4), 0 8px 20px rgba(0,0,0,.5);
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font-size: 15px; line-height: 1.45; transition: background 0.15s, color 0.15s; }
a { color: var(--accent); }
[dir="auto"] { unicode-bidi: plaintext; }

.theme-toggle {
  background: var(--surface); border: 1px solid var(--border); color: var(--text);
  border-radius: 999px; width: 2.1rem; height: 2.1rem; cursor: pointer; font-size: 1rem;
  display: inline-flex; align-items: center; justify-content: center; padding: 0;
}
.theme-toggle:hover { border-color: var(--accent); }

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

.panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); box-shadow: var(--shadow); }
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
.searchbar input[type="text"] {
  flex: 1; padding: 0.75rem 1.1rem; font-size: 1rem; border: 1px solid var(--border);
  border-radius: 999px; background: var(--surface); color: var(--text);
}
.searchbar input[type="text"]:focus { outline: 2px solid var(--accent); border-color: var(--accent); }
button.primary {
  background: var(--accent); color: #fff; border: none; border-radius: 999px;
  padding: 0.75rem 1.5rem; font-size: 0.95rem; font-weight: 700; cursor: pointer;
}
button.primary:hover { background: var(--accent-hover); }

.result-meta { color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.9rem; }

.venue-card {
  background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
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
    <button class="theme-toggle" id="themeToggle" aria-label="Toggle dark mode">&#127769;</button>
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
      <input type="text" id="q" placeholder="Search venues and items nationwide..." autofocus>
      <button class="primary" id="go">Search</button>
    </div>
    <div class="result-meta" id="resultMeta"></div>
    <div id="results"></div>
    <button class="primary load-more" id="loadMore" style="display:none; background:var(--surface); color:var(--accent); border:1px solid var(--border);">Load more</button>
  </div>
</div>

<script>
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const icon = theme === 'dark' ? '☀️' : '🌙';
  document.getElementById('themeToggle').innerHTML = `<span aria-hidden="true">${icon}</span>`;
  localStorage.setItem('wolt-il-theme', theme);
}
(function initTheme() {
  const saved = localStorage.getItem('wolt-il-theme');
  const theme = saved || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  applyTheme(theme);
})();
document.getElementById('themeToggle').addEventListener('click', () => {
  applyTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');
});

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
  button.start { background: var(--green); color: white; border: none; border-radius: 999px; padding: 0.4rem 0.9rem; cursor: pointer; font-weight: 700; }
  button.stop { background: var(--red); color: white; border: none; border-radius: 999px; padding: 0.4rem 0.9rem; cursor: pointer; font-weight: 700; }
  input.limit { width: 4.5rem; padding: 0.35rem; border: 1px solid var(--border); border-radius: var(--radius-sm); }
  .dot-status { display: inline-block; width: 0.55rem; height: 0.55rem; border-radius: 50%; margin-right: 0.4rem; }
  .dot-status.running { background: var(--green); }
  .dot-status.idle { background: #b0b3b8; }
  .dot-status.failed { background: var(--red); }
  pre { background: #0d1117; color: #d1d5db; padding: 0.7rem; border-radius: 8px; max-height: 260px; overflow: auto; font-size: 0.78rem; display: none; margin: 0.4rem 0 0 0; }
  pre.shown { display: block; }
  .toggle-log { font-size: 0.78rem; cursor: pointer; color: var(--accent); background: none; border: none; padding: 0; font-family: inherit; }
  .toggle-log:focus-visible { outline: 2px solid var(--accent); border-radius: 2px; }
  button.start:disabled, button.stop:disabled, .load-more:disabled { opacity: 0.6; cursor: not-allowed; }
  .job-desc { color: var(--text-muted); font-size: 0.78rem; margin-top: 0.15rem; }
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">Wolt IL <span class="dot">Search</span></div>
  <div style="display:flex; align-items:center; gap:1rem;">
    <button class="theme-toggle" id="themeToggle" aria-label="Toggle dark mode">&#127769;</button>
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
    <div class="panel-body">
      <table>
        <thead><tr><th>Job</th><th>Status</th><th>Limit</th><th></th></tr></thead>
        <tbody id="jobsTable"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const icon = theme === 'dark' ? '☀️' : '🌙';
  document.getElementById('themeToggle').innerHTML = `<span aria-hidden="true">${icon}</span>`;
  localStorage.setItem('wolt-il-theme', theme);
}
(function initTheme() {
  const saved = localStorage.getItem('wolt-il-theme');
  const theme = saved || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  applyTheme(theme);
})();
document.getElementById('themeToggle').addEventListener('click', () => {
  applyTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');
});

const JOB_DESCRIPTIONS = {
  'regions': 'Refresh the 24 Israel region list',
  'venues': 'Refresh restaurant venues (all regions)',
  'retail-venues': 'Refresh retail venues: electronics, general_merchandise, home_and_diy, ... (all regions)',
  'menus': 'Fetch restaurant menus (JSON path)',
  'menus-retail': 'Fetch general_merchandise/home_and_diy items (JSON path)',
  'electronics-items': 'Fetch electronics item catalogs (HTML scrape)',
  'retail-rescrape': 'Rescrape empty-catalog general_merchandise/home_and_diy venues (HTML scrape)',
  'reindex': 'Rebuild the FTS5 search index',
  'full': 'Regions + venues + a menu batch, in one shot',
};
const NO_LIMIT_JOBS = new Set(['reindex', 'regions', 'venues', 'retail-venues']);
// These hit Wolt for many venues and can run for minutes — confirm before starting.
const HEAVY_JOBS = new Set(['menus', 'menus-retail', 'electronics-items', 'retail-rescrape', 'full']);

function fmtStarted(ts) { return ts ? new Date(ts * 1000).toLocaleTimeString() : ''; }

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

  const jobsBody = document.getElementById('jobsTable');
  jobsBody.innerHTML = Object.entries(status.jobs).map(([name, s]) => {
    const dotClass = s.running ? 'running' : (s.returncode ? 'failed' : 'idle');
    const label = s.running ? `running since ${fmtStarted(s.started_at)}`
      : (s.returncode != null ? `exited (code ${s.returncode})` : 'idle');
    const logShown = document.getElementById(`log-${name}`)?.classList.contains('shown');
    return `
    <tr class="job-row" data-job="${name}">
      <td><strong>${name}</strong><div class="job-desc">${JOB_DESCRIPTIONS[name] || ''}</div></td>
      <td><span class="dot-status ${dotClass}"></span>${label}<br><button type="button" class="toggle-log" id="toggle-log-${name}" onclick="toggleLog('${name}')">${logShown ? 'hide log' : 'show log'}</button></td>
      <td>${NO_LIMIT_JOBS.has(name) ? '' : '<input type="number" class="limit" value="200" aria-label="Limit for ' + name + '">'}</td>
      <td>${s.running
        ? `<button class="stop" onclick="stopJob('${name}')">Stop</button>`
        : `<button class="start" onclick="startJob('${name}')">Start</button>`}</td>
    </tr>
    <tr><td colspan="4"><pre id="log-${name}" class="${logShown ? 'shown' : ''}"></pre></td></tr>
  `;
  }).join('');
}

async function startJob(name) {
  if (HEAVY_JOBS.has(name) && !confirm(`Start "${name}"? This hits Wolt for many venues and can run for several minutes.`)) {
    return;
  }
  const row = document.querySelector(`tr[data-job="${name}"]`);
  const limitInput = row.querySelector('.limit');
  const limit = limitInput ? limitInput.value : 200;
  const btn = row.querySelector('button.start');
  if (btn) { btn.disabled = true; btn.textContent = 'Starting...'; }
  await fetch(`/api/jobs/${name}/start?limit=${limit}&delay=1.5`, { method: 'POST' });
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

refreshStatus();
setInterval(refreshStatus, 5000);
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
