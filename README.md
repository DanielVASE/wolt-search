# wolt-il-search-mcp

Nationwide Israel search over Wolt, without ever touching an address field.

![Search UI showing faceted filters and results](docs/screenshot.png)

## Why this exists

Wolt's own search only looks at the one delivery region tied to your current
address, and its matching is picky (near-exact substrings, easily thrown off
by word order or typos). This project:

1. Crawls Wolt's public, unauthenticated consumer endpoints (the same ones
   wolt.com's web app calls — no API key exists for them) across **all 24 of
   Wolt's Israel delivery regions** (Tel Aviv, Jerusalem, Haifa, Beer Sheva,
   Eilat, etc. — pulled live from Wolt's own `/v1/cities`, not guessed).
2. Covers all three Wolt verticals: restaurants, retail (electronics,
   general merchandise, ...), and groceries/convenience (Wolt Market,
   AM:PM, ...) — Wolt organizes each under its own front-page section with
   no shared listing, so each needs its own crawl path.
3. Caches venues and full item catalogs locally in SQLite.
4. Runs real full-text search (SQLite FTS5 + BM25 ranking) over that local
   cache — instant, not scoped to any one region, and not thrown by word
   order the way Wolt's own in-app search is.
5. Exposes `search` as an MCP tool so you can just ask Claude things like
   *"find sushi places open right now in Haifa"*, *"who sells shakshuka near
   Jerusalem"*, or *"find a gaming laptop with an RTX 5070"*.

**Important — Wolt's ToS disallows automated access at scale.** This is
built for one personal user running an occasional, rate-limited, resumable
crawl — not a public scraping service. Keep `--delay` reasonable (1–2s+) and
don't run it continuously.

## Quick start (Docker)

```bash
docker compose up -d --build
```

That's it — a search + admin web UI is now at **http://localhost:8787/**
(bound to localhost only, on purpose — the admin API can start/stop crawl
jobs with no login, so `docker-compose.yml` publishes it as
`127.0.0.1:8787:8787` rather than to your whole LAN; change that line to
`8787:8787` if you actually want other devices on your network to reach it).
The cache starts empty (crawling is a deliberate, rate-limited step, not
something that happens automatically on container start — see "Building the
cache" below). Open **http://localhost:8787/admin** and hit Start on
`regions`, then `venues` + `retail-venues` + `grocery-venues` (fast, a couple
of minutes), then `menus` + `electronics-items` + `grocery-items` (slow,
hours the first time — but they run as background jobs, so leave the tab and
come back). All of this also works
directly via the API (`curl -X POST localhost:8787/api/jobs/venues/start`),
which is what the admin buttons call.

The SQLite cache, its FTS5 index, and job logs all live in the `/data`
volume (`wolt-data` in `docker-compose.yml`), so `docker compose down` /
image rebuilds don't lose indexed data — only `docker compose down -v` does.

Tagged releases are also published as pre-built images to GitHub Container
Registry (see `.github/workflows/docker-release.yml`), so you can skip the
local build:

```bash
docker pull ghcr.io/danielvase/wolt-search:latest
docker run -p 127.0.0.1:8787:8787 -v wolt-data:/data ghcr.io/danielvase/wolt-search:latest
```

## Setup (without Docker)

```bash
cd ~/wolt-il-search-mcp
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/wolt-il-webui   # same web UI, defaults to http://127.0.0.1:8787/
```

## Building the cache

The venue lists are cheap (24 requests per vertical, one per region). Item
catalogs are **not** — restaurant menus are one HTTP request per venue
(~5,600+ restaurants nationwide); electronics/retail/grocery items are
several page fetches per venue (they use a different, heavier crawl — see
"How it works"). Because of that, crawling is a separate, resumable CLI
step — never something an MCP tool call triggers inline.

```bash
# One-time: region list + every venue in every region (~2-5 min, occasional 429s are normal — just rerun, it's idempotent)
.venv/bin/wolt-il-refresh regions
.venv/bin/wolt-il-refresh venues          # restaurants
.venv/bin/wolt-il-refresh retail-venues   # electronics, general merchandise, ...
.venv/bin/wolt-il-refresh grocery-venues  # Wolt Market, AM:PM, mini-markets, ...

# Restaurant menus: one request per venue. Run in the background — this can take hours the first time.
nohup .venv/bin/wolt-il-refresh --delay 2.0 menus --limit 6000 --max-age-hours 999999 > menu_crawl.log 2>&1 &

# Electronics item catalogs: several page fetches per venue, slower. Also background it.
nohup .venv/bin/wolt-il-refresh --delay 2.0 electronics-items --limit 250 --max-age-hours 999999 > electronics_crawl.log 2>&1 &

# Grocery item catalogs: same mechanism as electronics-items, also background it.
nohup .venv/bin/wolt-il-refresh --delay 2.0 grocery-items --limit 250 --max-age-hours 999999 > grocery_crawl.log 2>&1 &

# Rebuild the FTS5 search index once crawling has made progress (search reads only from this index):
.venv/bin/wolt-il-refresh reindex
```

`general_merchandise`/`home_and_diy` venues are a mix of loading strategies —
most work through `menus` above, but some come back with an empty catalog and
need the same HTML scraper `electronics-items` uses. Follow up on just those:

```bash
.venv/bin/wolt-il-refresh retail-rescrape --limit 50 --product-lines general_merchandise,home_and_diy
```

Check progress any time:

```bash
.venv/bin/wolt-il-refresh status
# {'regions': 24, 'venues': 7273, 'venues_with_menu': 1200, 'menu_items': 71000}
```

### Keeping it fresh

Venue lists change slowly; item catalogs change more often (prices, deals,
new items). A reasonable cadence: re-run `venues`/`retail-venues`/`grocery-venues`
weekly, and re-run `menus`/`electronics-items`/`grocery-items` nightly (capped
`--limit` so each run finishes quickly) to refresh whatever's gone stale, then
`reindex`:

```bash
.venv/bin/wolt-il-refresh full --menu-limit 500   # regions + venues + a menu batch, reindexes itself
.venv/bin/wolt-il-refresh electronics-items --limit 100
.venv/bin/wolt-il-refresh grocery-items --limit 100
.venv/bin/wolt-il-refresh reindex
```

This is a good candidate for a nightly cron job / launchd job / Orca
automation.

## Web UI

`http://localhost:8787/` — free-text search with:
- Region + category checklists, open-now/min-rating/price-range filters, sort by relevance/rating/price
- **Faceted filtering computed from your actual results**: "Shops in these results" (click a shop once to require it, again to exclude it, again to clear) and "Item categories in these results" — both populate from what you searched, not a fixed list
- Every venue and item links out to the real wolt.com page
- Honest result counts ("Showing 20 of 1,131") with Load More pagination — nothing is silently capped
- Dark theme only, styled to match wolt.com's own production palette (`#009de0` brand blue, pure-black dark mode, pill-shaped buttons — pulled from wolt.com's actual CSS, not guessed)

`http://localhost:8787/admin` — cache stats by category, and Start/Stop/log-tail controls for every crawl job in `cli.py`, so you never need a terminal to (re)build the cache. Job tracking is in-memory in the webapp process: a crawl you started keeps running if you close the tab, but restarting the webapp process itself (not the Docker container — a container restart kills the crawl too) orphans it from the admin UI's view.

## Using it standalone (no Claude needed)

```bash
.venv/bin/wolt-il-refresh search "hummus" --open --min-rating 8
.venv/bin/wolt-il-refresh search "burger" --region tel-aviv
```

## Registering the MCP server with Claude Code

Add to your project or `~/.claude.json` MCP config:

```json
{
  "mcpServers": {
    "wolt-il": {
      "command": "/Users/danielva/wolt-il-search-mcp/.venv/bin/wolt-il-search-mcp"
    }
  }
}
```

For Claude Desktop, add the same block to
`~/Library/Application Support/Claude/claude_desktop_config.json`.

### Tools exposed

| Tool | Description |
|---|---|
| `search` | Full-text search venues + menu items nationwide (or within one region), from the local cache. |
| `list_regions` | The 24 Wolt Israel region slugs, for `search(region=...)`. |
| `cache_status` | Region/venue/menu counts, to sanity-check freshness. |
| `get_menu_live` | Live (uncached) menu fetch for one venue slug, for prices right before ordering. |

## How it works under the hood

- `client.py` — raw HTTP calls to `consumer-api.wolt.com` (`/v1/cities`,
  `/v1/pages/restaurants?lat=&lon=&radius=`, `/v1/pages/venue-list/{target}`
  for the retail and grocery verticals, and the per-venue assortment
  endpoint), with rate limiting and 429 backoff.
- `retail_scraper.py` — electronics and grocery venues use
  `loading_strategy: "partial"`: their item data isn't in any JSON API
  response at all, only the category tree. The real items are embedded as
  server-rendered React Query state in the venue's category page HTML
  (`wolt.com/.../venue/{slug}/items/{category-slug}`) — this scrapes that.
- `cache.py` — SQLite storage (`~/.wolt-il-search/cache.db` by default),
  including `venues_fts`/`items_fts`: external-content FTS5 virtual tables
  (`unicode61` tokenizer, BM25 ranking with per-column weights favoring name
  over tags/description) kept in sync via `rebuild_search_index()` rather
  than triggers — cheap enough to just rebuild after each crawl batch.
- `indexer.py` — the crawls: regions → venues per region (all three
  verticals) → menus per restaurant venue → items per electronics/grocery
  venue (same HTML-scraper code path, just filtered by `product_line`).
- `search.py` — token-based search: each query token becomes an FTS5 prefix
  match (`token*`), tried as AND-across-tokens first (precision) and relaxed
  to OR if that finds nothing (recall), ranked by BM25 with small boosts for
  rating and open/closed status. This is why it doesn't have the old
  character-fuzzy-matching failure mode of technical/brand terms (e.g. "RTX",
  "SSD") coincidentally string-matching unrelated venue names — FTS5 only
  matches real shared tokens. A `rapidfuzz` character-similarity pass is kept
  as a fallback purely for typo tolerance, used only when the FTS5 pass finds
  nothing at all.
- `server.py` — the FastMCP server wrapping `search.py` + a live menu lookup.
- `webapp.py` — the FastAPI web UI (search + admin pages, HTML/CSS/JS embedded
  as strings, no separate frontend build step). Admin crawl jobs run as
  subprocesses of this server (`python -m wolt_il_search.cli ...`) so a page
  reload doesn't kill them; their logs go to `$WOLT_IL_DB`'s directory under
  `job-logs/`.
- `Dockerfile` / `docker-compose.yml` — multi-stage build (build tools
  discarded from the final image), runs as a non-root user, cache lives on a
  named volume mounted at `/data` (`WOLT_IL_DB=/data/cache.db`).

Two deliberate findings baked into the design:
- Wolt scopes `/v1/pages/restaurants` results to one delivery region
  regardless of `radius` — a 60km radius from Tel Aviv returns the exact
  same venues as a 3km radius. There's no way to get nationwide coverage
  from a single query; you have to query once per region and merge, which is
  exactly what `indexer.py` does using the 24 region anchors from Wolt's own
  `/v1/cities`.
- The restaurant, retail, and grocery verticals are three separate
  front-page sections (`/v1/pages/restaurants` vs.
  `/v1/pages/venue-list/isr_retail_gm:{region}` vs.
  `/v1/pages/venue-list/g_retail_groceries:{region}`) with no shared
  listing — a crawl of one silently misses the others entirely, which is
  why `refresh_venues()`, `refresh_retail_venues()`, and
  `refresh_grocery_venues()` are three distinct functions. The grocery
  vertical isn't discoverable from `/v1/pages/restaurants` or
  `/v1/cities` at all — it only turned up by fetching Wolt's actual
  `/v1/pages/front` discovery endpoint (the one the web app itself calls
  to build its homepage) and diffing its section list against what this
  project already crawled. Note the target has no `isr_` prefix, unlike
  `isr_retail_gm` — `isr_g_retail_groceries` and `isr_retail_groceries`
  both 404; only the bare `g_retail_groceries` works.
