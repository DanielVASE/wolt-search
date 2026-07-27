"""SQLite-backed local cache of Wolt Israel regions, venues, and menu items."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

DEFAULT_DB_PATH = Path.home() / ".wolt-il-search" / "cache.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS regions (
    slug TEXT PRIMARY KEY,
    name TEXT,
    lat REAL,
    lon REAL,
    updated_at REAL
);

CREATE TABLE IF NOT EXISTS venues (
    slug TEXT PRIMARY KEY,
    region_slug TEXT,
    name TEXT,
    tags TEXT,
    short_description TEXT,
    address TEXT,
    rating REAL,
    rating_volume INTEGER,
    online INTEGER,
    product_line TEXT,
    price_range INTEGER,
    currency TEXT,
    lat REAL,
    lon REAL,
    preview_items TEXT,
    updated_at REAL,
    menu_fetched_at REAL
);
CREATE INDEX IF NOT EXISTS idx_venues_region ON venues(region_slug);

-- Which Wolt discovery target (front-page vertical / category) a venue was
-- found under. A venue can legitimately appear under several, hence the
-- composite key. This is what lets the admin UI say "isr_retail_gm covers
-- these 8 product lines, 839 of them still pending" without hardcoding that
-- mapping in Python.
CREATE TABLE IF NOT EXISTS venue_sources (
    target TEXT,
    slug TEXT,
    updated_at REAL,
    PRIMARY KEY (target, slug)
);
CREATE INDEX IF NOT EXISTS idx_venue_sources_slug ON venue_sources(slug);

-- Crawl jobs, persisted so that restarting the webapp re-attaches to running
-- crawls (they're child processes with their own pid) and so finished runs
-- keep a summary instead of it vanishing with the truncated log file.
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT,
    title TEXT,
    params TEXT,
    state TEXT,
    pid INTEGER,
    log_path TEXT,
    progress TEXT,
    created_at REAL,
    finished_at REAL,
    returncode INTEGER
);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state, created_at DESC);

CREATE TABLE IF NOT EXISTS menu_items (
    id TEXT PRIMARY KEY,
    venue_slug TEXT,
    name TEXT,
    description TEXT,
    category_name TEXT,
    price INTEGER,
    original_price INTEGER,
    lowest_price INTEGER,
    enabled INTEGER,
    tags TEXT,
    updated_at REAL
);
CREATE INDEX IF NOT EXISTS idx_menu_items_venue ON menu_items(venue_slug);

-- External-content FTS5 indexes: map by column name onto venues/menu_items.
-- These are NOT auto-synced by SQLite — call Cache.rebuild_search_index()
-- after any batch of writes (every CLI refresh command does this).
CREATE VIRTUAL TABLE IF NOT EXISTS venues_fts USING fts5(
    name, tags, short_description,
    content='venues', content_rowid='rowid',
    tokenize="unicode61 remove_diacritics 2"
);

CREATE VIRTUAL TABLE IF NOT EXISTS items_fts USING fts5(
    name, category_name, description, tags,
    content='menu_items', content_rowid='rowid',
    tokenize="unicode61 remove_diacritics 2"
);
"""


class Cache:
    def __init__(self, db_path: Path | str | None = DEFAULT_DB_PATH):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # This DB is now hit concurrently: the webapp (one connection per
        # request) and one or more background crawler subprocesses, plus
        # occasional manual CLI use. Default rollback-journal mode takes a
        # single global write lock and fails fast with "database is locked";
        # WAL lets readers proceed during a writer and busy_timeout makes
        # writers retry/wait instead of erroring immediately on contention.
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        # busy_timeout first: switching journal_mode itself needs a moment of
        # exclusive access, so it must also benefit from the retry window —
        # setting it after would leave that one call racing other writers.
        self.conn.execute("PRAGMA busy_timeout=30000")
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            # The WAL switch itself needs a brief moment with no other writer
            # active; if another connection (e.g. an older crawler process
            # started before this switch was in place) is mid-write right
            # now, don't crash the whole app over a one-time transitional
            # race — busy_timeout alone still prevents most lock errors, and
            # the next Cache() to connect will retry the switch.
            pass
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        """Additive column migrations for caches created by older versions.

        This DB is a 200MB+ local artifact that took hours of rate-limited
        crawling to build — it gets migrated in place, never recreated.
        """
        existing = {row["name"] for row in self.conn.execute("PRAGMA table_info(venues)")}
        # Transient-failure bookkeeping. Previously any failed fetch called
        # mark_menu_fetched(), which stamped the venue as fresh and hid it
        # from the next crawl for the whole max-age window (a week by
        # default) with zero items. Tracking failures separately lets a
        # transient 429/timeout be retried with per-venue backoff instead.
        for column, ddl in (
            ("fetch_fail_count", "ALTER TABLE venues ADD COLUMN fetch_fail_count INTEGER DEFAULT 0"),
            ("last_error", "ALTER TABLE venues ADD COLUMN last_error TEXT"),
            ("last_error_at", "ALTER TABLE venues ADD COLUMN last_error_at REAL"),
            # When the HTML-scraper fallback last ran for this venue. Distinct
            # from menu_fetched_at: a venue can be successfully fetched via
            # the JSON path yet come back empty, which is precisely the case
            # the fallback exists for.
            ("scrape_attempted_at", "ALTER TABLE venues ADD COLUMN scrape_attempted_at REAL"),
        ):
            if column not in existing:
                self.conn.execute(ddl)
        self._backfill_venue_sources()

    def _backfill_venue_sources(self) -> None:
        """Seed venue->discovery-target provenance for pre-existing caches.

        New crawls record this as they go (see indexer.refresh_venue_discovery),
        but a cache built by an older version has thousands of venues with no
        recorded source, which would make the admin category picker show 0
        venues against every vertical. The historical mapping is knowable
        exactly, because until now there were only three discovery paths and
        each fed a fixed set of product lines.
        """
        if self.get_meta("venue_sources_backfilled_v1"):
            return
        self.conn.execute(
            """
            INSERT OR IGNORE INTO venue_sources(target, slug, updated_at)
            SELECT CASE
                     WHEN lower(product_line) = 'restaurant' THEN 'restaurants'
                     WHEN lower(product_line) = 'grocery' THEN 'g_retail_groceries'
                     ELSE 'isr_retail_gm'
                   END,
                   slug, ?
            FROM venues WHERE product_line IS NOT NULL
            """,
            (time.time(),),
        )
        self.conn.commit()
        self.set_meta("venue_sources_backfilled_v1", str(time.time()))

    def close(self) -> None:
        self.conn.close()

    # -- meta -----------------------------------------------------------
    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    # -- regions ----------------------------------------------------------
    def upsert_region(self, slug: str, name: str, lat: float, lon: float) -> None:
        self.conn.execute(
            """
            INSERT INTO regions(slug, name, lat, lon, updated_at) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET name=excluded.name, lat=excluded.lat, lon=excluded.lon, updated_at=excluded.updated_at
            """,
            (slug, name, lat, lon, time.time()),
        )
        self.conn.commit()

    def list_regions(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM regions ORDER BY name").fetchall()

    # -- venues -----------------------------------------------------------
    def upsert_venue(self, region_slug: str, venue: dict) -> None:
        rating = venue.get("rating") or {}
        loc = venue.get("location") or [None, None]
        self.conn.execute(
            """
            INSERT INTO venues(
                slug, region_slug, name, tags, short_description, address,
                rating, rating_volume, online, product_line, price_range,
                currency, lat, lon, preview_items, updated_at, menu_fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(
                (SELECT menu_fetched_at FROM venues WHERE slug = ?), NULL
            ))
            ON CONFLICT(slug) DO UPDATE SET
                region_slug=excluded.region_slug, name=excluded.name, tags=excluded.tags,
                short_description=excluded.short_description, address=excluded.address,
                rating=excluded.rating, rating_volume=excluded.rating_volume, online=excluded.online,
                product_line=excluded.product_line, price_range=excluded.price_range,
                currency=excluded.currency, lat=excluded.lat, lon=excluded.lon,
                preview_items=excluded.preview_items, updated_at=excluded.updated_at
            """,
            (
                venue.get("slug"),
                region_slug,
                venue.get("name"),
                json.dumps(venue.get("tags") or [], ensure_ascii=False),
                venue.get("short_description"),
                venue.get("address"),
                rating.get("score"),
                rating.get("volume"),
                1 if venue.get("online") else 0,
                venue.get("product_line"),
                venue.get("price_range"),
                venue.get("currency"),
                loc[1],
                loc[0],
                json.dumps(venue.get("venue_preview_items") or [], ensure_ascii=False),
                time.time(),
                venue.get("slug"),
            ),
        )

    def commit(self) -> None:
        self.conn.commit()

    def all_venues(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM venues").fetchall()

    def get_venue(self, slug: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM venues WHERE slug = ?", (slug,)).fetchone()

    # Electronics and grocery venues always use loading_strategy="partial"
    # (see retail_scraper.py) — the consumer-assortment JSON API returns zero
    # items for them regardless, so they're always routed to the HTML
    # scraper and excluded here to keep the two "needing menu" queries
    # mutually exclusive.
    #
    # general_merchandise (clothing/accessories) and home_and_diy
    # (furniture/home/decor) are a *mix* of loading strategies — most work
    # fine through the plain JSON path below, some come back empty and need
    # the scraper too. Those go through venues_with_empty_catalog() as a
    # targeted follow-up rather than being routed here unconditionally,
    # since the JSON path is much cheaper when it works.
    _IS_ELECTRONICS_SQL = """(
        lower(product_line) = 'electronics'
        OR lower(tags) LIKE '%"electronics"%'
        OR lower(tags) LIKE '%"computer"%'
    )"""
    _IS_GROCERY_SQL = "lower(product_line) = 'grocery'"
    _NEEDS_HTML_SCRAPER_SQL = f"({_IS_ELECTRONICS_SQL} OR {_IS_GROCERY_SQL})"

    # Per-venue retry backoff: a venue that just failed isn't retried
    # immediately, but it *is* retried — 15min, 1h, 2.25h, 4h ... capped at a
    # day. `?` is the current time, bound by _failure_gate_params().
    _RETRY_READY_SQL = """(
        COALESCE(fetch_fail_count, 0) = 0
        OR last_error_at IS NULL
        OR (? - last_error_at) > MIN(900 * fetch_fail_count * fetch_fail_count, 86400)
    )"""

    #: Give up on a venue after this many consecutive failures so a
    #: permanently broken slug can't eat a crawl's rate budget forever.
    MAX_FETCH_FAILURES = 10

    _NOT_EXHAUSTED_SQL = f"COALESCE(fetch_fail_count, 0) < {MAX_FETCH_FAILURES}"

    @staticmethod
    def _sql_limit(limit: int | None) -> int:
        """SQLite treats LIMIT -1 as unlimited, which is how "crawl the whole
        category" is expressed — the admin UI's default."""
        if limit is None or limit <= 0:
            return -1
        return limit

    def venues_needing_menu(
        self, max_age_seconds: float, limit: int, product_lines: tuple[str, ...] | None = None
    ) -> list[sqlite3.Row]:
        now = time.time()
        cutoff = now - max_age_seconds
        product_line_filter = ""
        params: tuple = (cutoff, now)
        if product_lines:
            placeholders = ",".join("?" for _ in product_lines)
            product_line_filter = f"AND product_line IN ({placeholders})"
            params = (cutoff, now, *product_lines)
        return self.conn.execute(
            f"""
            SELECT * FROM venues
            WHERE (menu_fetched_at IS NULL OR menu_fetched_at < ?)
              AND {self._RETRY_READY_SQL}
              AND {self._NOT_EXHAUSTED_SQL}
              AND NOT {self._NEEDS_HTML_SCRAPER_SQL}
              {product_line_filter}
            ORDER BY menu_fetched_at IS NOT NULL, menu_fetched_at ASC
            LIMIT ?
            """,
            (*params, self._sql_limit(limit)),
        ).fetchall()

    def retail_catalog_venues_needing_menu(
        self, max_age_seconds: float, limit: int, product_lines: tuple[str, ...] | None = None
    ) -> list[sqlite3.Row]:
        """Venues needing the HTML-scraper item crawl — electronics and
        grocery by default, narrowable to just one via product_lines.
        """
        now = time.time()
        cutoff = now - max_age_seconds
        product_line_filter = ""
        params: tuple = (cutoff, now)
        if product_lines:
            placeholders = ",".join("?" for _ in product_lines)
            product_line_filter = f"AND product_line IN ({placeholders})"
            params = (cutoff, now, *product_lines)
        return self.conn.execute(
            f"""
            SELECT * FROM venues
            WHERE (menu_fetched_at IS NULL OR menu_fetched_at < ?)
              AND {self._RETRY_READY_SQL}
              AND {self._NOT_EXHAUSTED_SQL}
              AND {self._NEEDS_HTML_SCRAPER_SQL}
              {product_line_filter}
            ORDER BY menu_fetched_at IS NOT NULL, menu_fetched_at ASC
            LIMIT ?
            """,
            (*params, self._sql_limit(limit)),
        ).fetchall()

    def venues_with_empty_catalog(
        self, product_lines: tuple[str, ...], limit: int, max_age_seconds: float
    ) -> list[sqlite3.Row]:
        """Venues already run through the plain JSON menu path that came back
        with zero items — candidates for the HTML scraper follow-up pass.

        Staleness is gated on `scrape_attempted_at`, not `menu_fetched_at`.
        Using the latter (as this did originally) made the fallback unable to
        pick up venues the *same crawl* had just emptied seconds earlier:
        the JSON phase stamps menu_fetched_at = now, so `menu_fetched_at <
        now - max_age` excluded exactly the venues that most needed scraping.
        Keying on the scraper's own last attempt fixes that while still not
        re-scraping a venue that is simply, genuinely empty.
        """
        now = time.time()
        cutoff = now - max_age_seconds
        placeholders = ",".join("?" for _ in product_lines)
        return self.conn.execute(
            f"""
            SELECT v.* FROM venues v
            WHERE v.product_line IN ({placeholders})
              AND v.menu_fetched_at IS NOT NULL
              AND (v.scrape_attempted_at IS NULL OR v.scrape_attempted_at < ?)
              AND {self._RETRY_READY_SQL}
              AND {self._NOT_EXHAUSTED_SQL}
              AND NOT EXISTS (SELECT 1 FROM menu_items mi WHERE mi.venue_slug = v.slug)
            ORDER BY v.scrape_attempted_at IS NOT NULL, v.menu_fetched_at ASC
            LIMIT ?
            """,
            (*product_lines, cutoff, now, self._sql_limit(limit)),
        ).fetchall()

    def mark_scrape_attempted(self, slug: str) -> None:
        self.conn.execute("UPDATE venues SET scrape_attempted_at = ? WHERE slug = ?", (time.time(), slug))

    def mark_menu_stale(self, slug: str) -> None:
        self.conn.execute("UPDATE venues SET menu_fetched_at = NULL WHERE slug = ?", (slug,))

    def mark_menu_fetched(self, slug: str) -> None:
        """Record a *successful* fetch, clearing any failure backoff."""
        self.conn.execute(
            "UPDATE venues SET menu_fetched_at = ?, fetch_fail_count = 0, last_error = NULL, last_error_at = NULL"
            " WHERE slug = ?",
            (time.time(), slug),
        )

    def record_fetch_failure(self, slug: str, error: str, permanent: bool = False) -> int:
        """Record a failed fetch without pretending the venue is up to date.

        A permanent failure (404/410/invalid slug) also stamps menu_fetched_at
        so the venue drops out of the candidate queries — that's what the old
        unconditional mark_menu_fetched() was *trying* to achieve, but it
        applied the same treatment to transient errors too.
        """
        now = time.time()
        if permanent:
            self.conn.execute(
                """UPDATE venues SET menu_fetched_at = ?, last_error = ?, last_error_at = ?,
                          fetch_fail_count = ? WHERE slug = ?""",
                (now, error[:500], now, self.MAX_FETCH_FAILURES, slug),
            )
        else:
            self.conn.execute(
                """UPDATE venues SET fetch_fail_count = COALESCE(fetch_fail_count, 0) + 1,
                          last_error = ?, last_error_at = ? WHERE slug = ?""",
                (error[:500], now, slug),
            )
        row = self.conn.execute("SELECT fetch_fail_count c FROM venues WHERE slug = ?", (slug,)).fetchone()
        return row["c"] if row else 0

    # -- discovery provenance ------------------------------------------------
    def record_venue_source(self, target: str, slug: str) -> None:
        self.conn.execute(
            "INSERT INTO venue_sources(target, slug, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(target, slug) DO UPDATE SET updated_at=excluded.updated_at",
            (target, slug, time.time()),
        )

    def product_lines_by_discovery_target(self) -> dict[str, tuple[str, ...]]:
        """target -> product lines observed under it, learned from crawls."""
        rows = self.conn.execute(
            """
            SELECT s.target, v.product_line, COUNT(*) c
            FROM venue_sources s JOIN venues v ON v.slug = s.slug
            WHERE v.product_line IS NOT NULL
            GROUP BY s.target, v.product_line
            HAVING c > 0
            ORDER BY c DESC
            """
        ).fetchall()
        out: dict[str, list[str]] = {}
        for row in rows:
            out.setdefault(row["target"], []).append(row["product_line"])
        return {k: tuple(v) for k, v in out.items()}

    def product_line_stats(self) -> list[dict]:
        """Per-product-line crawl progress, including the denominators the
        admin UI needs to draw a progress bar (pending / failing), which
        `counts()` never exposed."""
        rows = self.conn.execute(
            f"""
            SELECT product_line,
                   COUNT(*) total,
                   SUM(CASE WHEN menu_fetched_at IS NOT NULL THEN 1 ELSE 0 END) fetched,
                   SUM(CASE WHEN EXISTS (
                         SELECT 1 FROM menu_items mi WHERE mi.venue_slug = venues.slug
                       ) THEN 1 ELSE 0 END) with_items,
                   SUM(CASE WHEN COALESCE(fetch_fail_count, 0) > 0
                             AND COALESCE(fetch_fail_count, 0) < {self.MAX_FETCH_FAILURES}
                            THEN 1 ELSE 0 END) retrying,
                   SUM(CASE WHEN COALESCE(fetch_fail_count, 0) >= {self.MAX_FETCH_FAILURES}
                            THEN 1 ELSE 0 END) gave_up,
                   SUM(CASE WHEN scrape_attempted_at IS NULL
                             AND menu_fetched_at IS NOT NULL
                             AND NOT EXISTS (
                                   SELECT 1 FROM menu_items mi WHERE mi.venue_slug = venues.slug
                                 )
                            THEN 1 ELSE 0 END) scrapable
            FROM venues GROUP BY product_line ORDER BY total DESC
            """
        ).fetchall()
        stats = []
        for row in rows:
            item = dict(row)
            # "Fetched" and "has items" are different things: a venue can be
            # fetched successfully and still hold zero items (retail venues
            # whose catalog only exists in HTML). Reporting only fetch-due
            # venues as pending made whole product lines look complete at
            # 163/333, so count the fetched-but-empty ones as work too — they
            # are exactly what the scrape fallback picks up.
            item["empty"] = max(0, item["fetched"] - item["with_items"])
            item["pending"] = max(0, item["total"] - item["fetched"]) + item["scrapable"]
            stats.append(item)
        return stats

    # -- jobs ---------------------------------------------------------------
    def insert_job(self, job: dict) -> None:
        self.conn.execute(
            """
            INSERT INTO jobs(id, kind, title, params, state, pid, log_path, progress, created_at)
            VALUES (:id, :kind, :title, :params, :state, :pid, :log_path, :progress, :created_at)
            """,
            job,
        )
        self.conn.commit()

    def update_job(self, job_id: str, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{k} = :{k}" for k in fields)
        self.conn.execute(f"UPDATE jobs SET {assignments} WHERE id = :id", {**fields, "id": job_id})
        self.conn.commit()

    def list_jobs(self, limit: int = 40) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM jobs ORDER BY (state = 'running') DESC, created_at DESC LIMIT ?", (limit,)
        ).fetchall()

    def get_job(self, job_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    def prune_jobs(self, keep: int = 40) -> None:
        self.conn.execute(
            "DELETE FROM jobs WHERE state != 'running' AND id NOT IN"
            " (SELECT id FROM jobs WHERE state != 'running' ORDER BY created_at DESC LIMIT ?)",
            (keep,),
        )
        self.conn.commit()

    # -- menu items ---------------------------------------------------------
    def replace_menu_items(self, venue_slug: str, items: list[dict]) -> None:
        self.conn.execute("DELETE FROM menu_items WHERE venue_slug = ?", (venue_slug,))
        now = time.time()
        for it in items:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO menu_items(
                    id, venue_slug, name, description, category_name, price,
                    original_price, lowest_price, enabled, tags, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    it["id"],
                    venue_slug,
                    it.get("name"),
                    it.get("description"),
                    it.get("category_name"),
                    it.get("price"),
                    it.get("original_price"),
                    it.get("lowest_price"),
                    1 if it.get("enabled") else 0,
                    json.dumps(it.get("tags") or [], ensure_ascii=False),
                    now,
                ),
            )

    def menu_items_for_venue(self, venue_slug: str) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM menu_items WHERE venue_slug = ?", (venue_slug,)).fetchall()

    def all_menu_items(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM menu_items").fetchall()

    # -- full-text search ---------------------------------------------------
    def rebuild_search_index(self) -> None:
        """Resync venues_fts/items_fts from their content tables. Cheap enough
        (well under a second at this dataset's size) to call after every
        batch of writes rather than maintaining incremental triggers.
        """
        self.conn.execute("INSERT INTO venues_fts(venues_fts) VALUES('rebuild')")
        self.conn.execute("INSERT INTO items_fts(items_fts) VALUES('rebuild')")
        self.conn.commit()

    def search_venues_fts(self, match_query: str, limit: int = 500) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT venues.*, -bm25(venues_fts, 10.0, 5.0, 2.0) AS text_score
            FROM venues_fts JOIN venues ON venues.rowid = venues_fts.rowid
            WHERE venues_fts MATCH ?
            ORDER BY text_score DESC
            LIMIT ?
            """,
            (match_query, limit),
        ).fetchall()

    def search_items_fts(self, match_query: str, limit: int = 1000) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT menu_items.*, -bm25(items_fts, 10.0, 4.0, 1.0, 3.0) AS text_score
            FROM items_fts JOIN menu_items ON menu_items.rowid = items_fts.rowid
            WHERE items_fts MATCH ? AND menu_items.enabled = 1
            ORDER BY text_score DESC
            LIMIT ?
            """,
            (match_query, limit),
        ).fetchall()

    def counts(self) -> dict:
        n_regions = self.conn.execute("SELECT COUNT(*) c FROM regions").fetchone()["c"]
        n_venues = self.conn.execute("SELECT COUNT(*) c FROM venues").fetchone()["c"]
        n_with_menu = self.conn.execute("SELECT COUNT(*) c FROM venues WHERE menu_fetched_at IS NOT NULL").fetchone()["c"]
        n_items = self.conn.execute("SELECT COUNT(*) c FROM menu_items").fetchone()["c"]
        # Pending/failing were missing entirely, which is why the admin page
        # could never show progress against a denominator.
        n_pending = self.conn.execute("SELECT COUNT(*) c FROM venues WHERE menu_fetched_at IS NULL").fetchone()["c"]
        n_failing = self.conn.execute(
            f"SELECT COUNT(*) c FROM venues WHERE COALESCE(fetch_fail_count, 0) BETWEEN 1 AND {self.MAX_FETCH_FAILURES - 1}"
        ).fetchone()["c"]
        n_gave_up = self.conn.execute(
            f"SELECT COUNT(*) c FROM venues WHERE COALESCE(fetch_fail_count, 0) >= {self.MAX_FETCH_FAILURES}"
        ).fetchone()["c"]
        return {
            "regions": n_regions,
            "venues": n_venues,
            "venues_with_menu": n_with_menu,
            "menu_items": n_items,
            "venues_pending": n_pending,
            "venues_retrying": n_failing,
            "venues_gave_up": n_gave_up,
        }
