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
        self.conn.commit()

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

    def venues_needing_menu(
        self, max_age_seconds: float, limit: int, product_lines: tuple[str, ...] | None = None
    ) -> list[sqlite3.Row]:
        cutoff = time.time() - max_age_seconds
        product_line_filter = ""
        params: tuple = (cutoff,)
        if product_lines:
            placeholders = ",".join("?" for _ in product_lines)
            product_line_filter = f"AND product_line IN ({placeholders})"
            params = (cutoff, *product_lines)
        return self.conn.execute(
            f"""
            SELECT * FROM venues
            WHERE (menu_fetched_at IS NULL OR menu_fetched_at < ?)
              AND NOT {self._NEEDS_HTML_SCRAPER_SQL}
              {product_line_filter}
            ORDER BY menu_fetched_at IS NOT NULL, menu_fetched_at ASC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

    def retail_catalog_venues_needing_menu(
        self, max_age_seconds: float, limit: int, product_lines: tuple[str, ...] | None = None
    ) -> list[sqlite3.Row]:
        """Venues needing the HTML-scraper item crawl — electronics and
        grocery by default, narrowable to just one via product_lines.
        """
        cutoff = time.time() - max_age_seconds
        product_line_filter = ""
        params: tuple = (cutoff,)
        if product_lines:
            placeholders = ",".join("?" for _ in product_lines)
            product_line_filter = f"AND product_line IN ({placeholders})"
            params = (cutoff, *product_lines)
        return self.conn.execute(
            f"""
            SELECT * FROM venues
            WHERE (menu_fetched_at IS NULL OR menu_fetched_at < ?)
              AND {self._NEEDS_HTML_SCRAPER_SQL}
              {product_line_filter}
            ORDER BY menu_fetched_at IS NOT NULL, menu_fetched_at ASC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

    def venues_with_empty_catalog(
        self, product_lines: tuple[str, ...], limit: int, max_age_seconds: float
    ) -> list[sqlite3.Row]:
        """Venues already run through the plain JSON menu path that came back
        with zero items — candidates for the HTML scraper follow-up pass.

        Gated by max_age_seconds like the sibling "needing menu" queries,
        otherwise a venue that's genuinely empty (delisted, out of stock)
        gets rescraped on every single run forever.
        """
        cutoff = time.time() - max_age_seconds
        placeholders = ",".join("?" for _ in product_lines)
        return self.conn.execute(
            f"""
            SELECT v.* FROM venues v
            WHERE v.product_line IN ({placeholders})
              AND v.menu_fetched_at IS NOT NULL AND v.menu_fetched_at < ?
              AND NOT EXISTS (SELECT 1 FROM menu_items mi WHERE mi.venue_slug = v.slug)
            LIMIT ?
            """,
            (*product_lines, cutoff, limit),
        ).fetchall()

    def mark_menu_stale(self, slug: str) -> None:
        self.conn.execute("UPDATE venues SET menu_fetched_at = NULL WHERE slug = ?", (slug,))

    def mark_menu_fetched(self, slug: str) -> None:
        self.conn.execute("UPDATE venues SET menu_fetched_at = ? WHERE slug = ?", (time.time(), slug))

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
        return {
            "regions": n_regions,
            "venues": n_venues,
            "venues_with_menu": n_with_menu,
            "menu_items": n_items,
        }
