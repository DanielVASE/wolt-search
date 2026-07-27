"""Local web UI: a real search page (multi-select filters, price range,
include/exclude shops, sort, honest result counts, clickable Wolt links)
plus an admin page to create/monitor crawl jobs.

The admin page is one job creator + one list of jobs, not a grid of fixed
buttons. You pick categories (discovered from Wolt's own API), press Start,
and the whole selection is crawled — no venue cap to set, with the tuning
knobs folded into an "Advanced settings" disclosure for the rare case you
want them.

Crawl jobs run as subprocesses of this server (`python -m
wolt_il_search.cli crawl ...`) so a page reload or browser close doesn't kill
them. They emit NDJSON progress events on stdout, which this module folds
into live per-job progress (phase, current venue, X/Y, ETA, retries) and
persists to the `jobs` table so a webapp restart re-attaches instead of
orphaning them.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .cache import DEFAULT_DB_PATH, Cache
from .categories import build_catalog, discover
from .client import WoltClient
from .search import SearchFilters, search_full

LOG = logging.getLogger("wolt_il_search.webapp")

DB_PATH = os.environ.get("WOLT_IL_DB")
JOB_LOG_DIR = (Path(DB_PATH).parent if DB_PATH else DEFAULT_DB_PATH.parent) / "job-logs"

#: Cap on simultaneously running crawls. Several jobs are useful (discover one
#: vertical while items crawl for another), but every extra job multiplies the
#: request rate against Wolt, which is exactly what the pacing logic exists to
#: keep polite.
MAX_CONCURRENT_JOBS = 3

#: Display lines kept per job for the live log pane. Bounded so a multi-hour
#: crawl can't grow the webapp's memory without limit; the full log is always
#: on disk.
LOG_TAIL_LINES = 500


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


@dataclasses.dataclass
class CrawlParams:
    """Validated, clamped crawl settings.

    The admin API has no auth (see README) so nothing here trusts the caller:
    every number is clamped and every category key is checked against the
    live catalog before it can reach argv.
    """

    categories: list[str]
    phases: tuple[str, ...] = ("discover", "items")
    limit: int = 0  # 0 = the whole category, which is now the default
    max_age_hours: float = 168.0
    delay: float = 1.2
    max_delay: float = 60.0
    max_attempts: int = 4
    force: bool = False

    @classmethod
    def from_request(cls, body: dict, valid_keys: set[str]) -> CrawlParams:
        raw_categories = body.get("categories") or []
        if isinstance(raw_categories, str):
            raw_categories = [c for c in raw_categories.split(",") if c]
        categories = [c for c in raw_categories if c in valid_keys]
        if not categories:
            raise HTTPException(400, "select at least one known category")

        raw_phases = body.get("phases") or ["discover", "items"]
        phases = tuple(p for p in raw_phases if p in ("discover", "items"))
        if not phases:
            raise HTTPException(400, "phases must include 'discover' and/or 'items'")

        limit = int(body.get("limit") or 0)
        return cls(
            categories=categories,
            phases=phases,
            # Negative would be meaningless; 0 stays 0 (unbounded) and any
            # positive value is capped so a typo can't queue a million venues.
            limit=0 if limit <= 0 else int(_clamp(limit, 1, 100_000)),
            max_age_hours=_clamp(float(body.get("max_age_hours", 168.0)), 0.0, 24 * 365),
            # Pacing floor stays >= 0.5s: this is a politeness guarantee
            # toward Wolt, not a user preference.
            delay=_clamp(float(body.get("delay", 1.2)), 0.5, 30.0),
            max_delay=_clamp(float(body.get("max_delay", 60.0)), 5.0, 300.0),
            max_attempts=int(_clamp(int(body.get("max_attempts", 4)), 1, 8)),
            force=bool(body.get("force", False)),
        )

    def to_argv(self) -> list[str]:
        argv = [
            "--delay",
            str(self.delay),
            "--max-delay",
            str(self.max_delay),
            "--max-attempts",
            str(self.max_attempts),
            "crawl",
            "--categories",
            ",".join(self.categories),
            "--phases",
            ",".join(self.phases),
            "--limit",
            str(self.limit),
            "--max-age-hours",
            str(self.max_age_hours),
            "--progress",
            "json",
        ]
        if self.force:
            argv.append("--force")
        return argv


class _Job:
    """One crawl process plus the progress state derived from its output.

    `proc` is None for a job this webapp process didn't start but re-attached
    to after a restart (we have its pid from the DB but no Popen handle).
    """

    def __init__(self, job_id: str, kind: str, title: str, params: dict, log_path: Path, pid: int, proc=None):
        self.id = job_id
        self.kind = kind
        self.title = title
        self.params = params
        self.log_path = log_path
        self.pid = pid
        self.proc = proc
        self.created_at = time.time()
        self.finished_at: float | None = None
        self.returncode: int | None = None
        self.state = "running"
        self.offset = 0  # byte offset already consumed from the log
        self._pending = b""  # partial trailing line between reads
        self.seq = 0
        self.lines: deque[dict] = deque(maxlen=LOG_TAIL_LINES)
        self.progress: dict = {
            "plan": [],
            "phase": None,
            "phase_index": 0,
            "phase_of": 0,
            "current": None,
            "i": 0,
            "n": 0,
            "ok": 0,
            "failed": 0,
            "items": 0,
            # Totals for phases that already finished. The per-phase counters
            # above reset at every phase boundary (they have to, they drive
            # the phase progress bar), so without these a job whose last
            # phase had nothing to do would end up reporting zero work done.
            "cum_ok": 0,
            "cum_failed": 0,
            "cum_items": 0,
            "retries": 0,
            "last_retry": None,
            "last_error": None,
            "phase_started_at": None,
            "rate": None,
            "eta_seconds": None,
            "summary": None,
        }

    # -- process state ----------------------------------------------------
    def is_running(self) -> bool:
        if self.proc is not None:
            return self.proc.poll() is None
        if self.pid <= 0:
            return False
        try:
            os.kill(self.pid, 0)  # signal 0 = existence check only
        except (ProcessLookupError, PermissionError):
            return False
        except OSError:
            return False
        return True

    def poll_exit(self) -> int | None:
        if self.proc is not None:
            return self.proc.poll()
        return None if self.is_running() else -1

    def stop(self) -> bool:
        if not self.is_running():
            return False
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
            return True
        try:
            os.kill(self.pid, signal.SIGTERM)
        except OSError:
            return False
        return True

    # -- output consumption ----------------------------------------------
    def _add_line(self, text: str) -> None:
        self.seq += 1
        self.lines.append({"seq": self.seq, "text": text})

    def drain(self) -> None:
        """Consume whatever the child has written since the last call.

        Incremental by byte offset. The old admin page re-read the *entire*
        log file every 5s per open pane and then threw away all but the last
        100 lines; on a long crawl that's megabytes of pointless I/O per poll.
        """
        try:
            size = self.log_path.stat().st_size
        except FileNotFoundError:
            return
        if size < self.offset:  # log was truncated/rotated under us
            self.offset = 0
            self._pending = b""
        if size == self.offset:
            return
        with open(self.log_path, "rb") as fh:
            fh.seek(self.offset)
            chunk = fh.read()
        self.offset += len(chunk)
        buffer = self._pending + chunk
        *complete, self._pending = buffer.split(b"\n")
        for raw in complete:
            line = raw.decode("utf-8", errors="replace").rstrip("\r")
            if not line.strip():
                continue
            if line.startswith("{"):
                try:
                    self._apply_event(json.loads(line))
                    continue
                except ValueError:
                    pass  # not an event after all; show it verbatim
            self._add_line(line)

    def _apply_event(self, event: dict) -> None:
        """Fold one NDJSON progress event into this job's live state."""
        kind = event.get("ev")
        p = self.progress
        if kind == "plan":
            p["plan"] = event.get("steps") or []
            self._add_line("plan: " + " → ".join(p["plan"]))
        elif kind == "phase":
            p["cum_ok"] += p["ok"]
            p["cum_failed"] += p["failed"]
            p["cum_items"] += p["items"]
            p["phase"] = event.get("name")
            p["phase_index"] = event.get("index") or 0
            p["phase_of"] = event.get("of") or 0
            p["n"] = event.get("total") or 0
            p["i"] = p["ok"] = p["failed"] = p["items"] = 0
            p["current"] = None
            p["phase_started_at"] = time.time()
            p["rate"] = p["eta_seconds"] = None
            self._add_line(f"== {p['phase']}")
        elif kind == "phase_total":
            p["n"] = event.get("total") or 0
        elif kind == "current":
            p["current"] = event.get("label")
            p["i"] = event.get("i") or p["i"]
            if event.get("n"):
                p["n"] = event["n"]
        elif kind == "tick":
            p["i"] = event.get("i") or p["i"]
            if event.get("n"):
                p["n"] = event["n"]
            p["ok"] = event.get("okc", p["ok"])
            p["failed"] = event.get("failc", p["failed"])
            p["items"] = event.get("itemc", p["items"])
            label = event.get("label")
            if event.get("ok"):
                self._add_line(f"  ✓ {label} — {event.get('items') if event.get('items') is not None else 0} items")
            else:
                p["last_error"] = f"{label}: {event.get('error') or 'failed'}"
                mark = "permanent" if event.get("permanent") else "will retry later"
                self._add_line(f"  ✗ {label} — {str(event.get('error') or '')[:160]} ({mark})")
            self._recompute_eta()
        elif kind == "retry":
            p["retries"] += 1
            p["last_retry"] = {
                "reason": event.get("reason"),
                "wait": event.get("wait"),
                "pacing": event.get("pacing"),
                "attempt": event.get("attempt"),
                "of": event.get("of"),
                "at": event.get("t"),
            }
            self._add_line(
                f"  ⏳ {event.get('reason')} — backing off {event.get('wait')}s"
                f" (attempt {event.get('attempt')}/{event.get('of')}, pacing {event.get('pacing')}s)"
            )
        elif kind == "done":
            p["summary"] = event.get("summary")
            p["current"] = None
            self._add_line(f"done in {event.get('elapsed', 0):.0f}s")
        elif kind == "log":
            self._add_line(str(event.get("msg", "")))

    def _recompute_eta(self) -> None:
        p = self.progress
        started = p.get("phase_started_at")
        if not started or not p.get("i"):
            return
        elapsed = time.time() - started
        if elapsed <= 0:
            return
        rate = p["i"] / elapsed  # venues per second
        p["rate"] = round(rate, 3)
        remaining = max(0, (p.get("n") or 0) - p["i"])
        p["eta_seconds"] = round(remaining / rate) if rate > 0 and remaining else 0

    # -- serialization -----------------------------------------------------
    def to_json(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "title": self.title,
            "params": self.params,
            "state": self.state,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "returncode": self.returncode,
            "seq": self.seq,
            "progress": self.progress,
        }


class JobManager:
    """Tracks crawl jobs by id, persists them, and re-attaches after restart.

    The previous version keyed jobs by *job name* in a plain dict, which made
    the eleven fixed jobs a hard limit (one run per name, no history) and lost
    all state when the webapp restarted — the README had to document that
    restarting orphans a running crawl.
    """

    def __init__(self) -> None:
        self.jobs: dict[str, _Job] = {}
        self._lock = threading.Lock()
        self._pump_thread: threading.Thread | None = None
        self._stop_pump = threading.Event()

    # -- lifecycle --------------------------------------------------------
    def start_background_pump(self) -> None:
        """Continuously fold child output into progress state.

        Progress must advance whether or not a browser is polling, so that
        (a) reopening the page shows current state immediately, and (b) a
        finished job is recorded even if nobody was watching.
        """
        if self._pump_thread and self._pump_thread.is_alive():
            return
        self._pump_thread = threading.Thread(target=self._pump_loop, name="job-pump", daemon=True)
        self._pump_thread.start()

    def _pump_loop(self) -> None:
        while not self._stop_pump.wait(0.5):
            try:
                self.pump()
            except Exception:  # noqa: BLE001 - a pump error must not kill the thread
                LOG.exception("job pump iteration failed")

    def pump(self) -> None:
        with self._lock:
            jobs = list(self.jobs.values())
        for job in jobs:
            if job.state != "running":
                continue
            job.drain()
            if not job.is_running():
                job.drain()  # final flush after exit
                code = job.poll_exit()
                job.returncode = code
                job.finished_at = time.time()
                if job.progress.get("summary") is not None:
                    job.state = "done"
                elif code == 0:
                    job.state = "done"
                elif code in (-signal.SIGTERM, -signal.SIGKILL, -15, -9):
                    job.state = "stopped"
                else:
                    job.state = "failed"
                self._persist(job)

    def reattach(self) -> None:
        """Pick running jobs back up after a webapp restart.

        A crawl is a child process writing to a log file, so as long as the
        pid is alive its whole progress history can be rebuilt by replaying
        the log from byte 0.
        """
        cache = Cache(DB_PATH)
        try:
            for row in cache.list_jobs(limit=MAX_CONCURRENT_JOBS * 4):
                if row["state"] != "running":
                    continue
                job = _Job(
                    row["id"],
                    row["kind"],
                    row["title"],
                    json.loads(row["params"] or "{}"),
                    Path(row["log_path"]),
                    row["pid"] or 0,
                )
                job.created_at = row["created_at"] or time.time()
                if job.is_running():
                    job.drain()  # replay the log to rebuild progress
                    with self._lock:
                        self.jobs[job.id] = job
                    LOG.info("re-attached to running job %s (pid %s)", job.id, job.pid)
                else:
                    # Died while we were down: keep whatever the log recorded
                    # rather than leaving a permanently "running" row.
                    job.drain()
                    job.state = "done" if job.progress.get("summary") else "orphaned"
                    job.finished_at = time.time()
                    with self._lock:
                        self.jobs[job.id] = job
                    self._persist(job)
        finally:
            cache.close()

    # -- starting ---------------------------------------------------------
    def running_count(self) -> int:
        with self._lock:
            return sum(1 for j in self.jobs.values() if j.state == "running")

    def start(self, kind: str, title: str, argv: list[str], params: dict) -> _Job:
        if self.running_count() >= MAX_CONCURRENT_JOBS:
            raise RuntimeError(f"already running {MAX_CONCURRENT_JOBS} jobs — stop one first")

        job_id = uuid.uuid4().hex[:12]
        JOB_LOG_DIR.mkdir(parents=True, exist_ok=True)
        # Job-id-scoped log files: the old code wrote {name}.log and truncated
        # it on every start, destroying the previous run's output.
        log_path = JOB_LOG_DIR / f"{job_id}.log"

        cmd = [sys.executable, "-m", "wolt_il_search.cli"]
        if DB_PATH:
            cmd += ["--db", DB_PATH]
        cmd += argv

        # Popen dup()s the fd into the child on launch, so the parent's handle
        # can close immediately — holding it open would leak one fd per job
        # start for the life of the webapp process.
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        job = _Job(job_id, kind, title, params, log_path, proc.pid, proc=proc)
        with self._lock:
            self.jobs[job_id] = job
        self._persist(job, insert=True)
        self.start_background_pump()
        return job

    def stop(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job:
            raise KeyError(job_id)
        stopped = job.stop()
        if stopped:
            job.state = "stopped"
            job.finished_at = time.time()
            self._persist(job)
        return stopped

    # -- persistence ------------------------------------------------------
    def _persist(self, job: _Job, insert: bool = False) -> None:
        cache = Cache(DB_PATH)
        try:
            payload = {
                "id": job.id,
                "kind": job.kind,
                "title": job.title,
                "params": json.dumps(job.params, ensure_ascii=False),
                "state": job.state,
                "pid": job.pid,
                "log_path": str(job.log_path),
                "progress": json.dumps(job.progress, ensure_ascii=False),
                "created_at": job.created_at,
            }
            if insert:
                cache.insert_job(payload)
                cache.prune_jobs()
            else:
                cache.update_job(
                    job.id,
                    state=job.state,
                    progress=payload["progress"],
                    finished_at=job.finished_at,
                    returncode=job.returncode,
                )
        finally:
            cache.close()

    def snapshot(self) -> list[dict]:
        with self._lock:
            jobs = sorted(
                self.jobs.values(),
                key=lambda j: (j.state != "running", -(j.created_at or 0)),
            )
        return [j.to_json() for j in jobs]

    def log_since(self, job_id: str, after_seq: int) -> dict:
        job = self.jobs.get(job_id)
        if not job:
            raise KeyError(job_id)
        job.drain()
        lines = [line for line in job.lines if line["seq"] > after_seq]
        # A client that fell far behind (pane closed for a while) may have
        # missed lines evicted from the ring buffer; tell it so it can note
        # the gap instead of silently showing a discontinuous log.
        oldest = job.lines[0]["seq"] if job.lines else 0
        return {
            "lines": lines,
            "seq": job.seq,
            "gap": after_seq > 0 and oldest > after_seq + 1,
            "state": job.state,
        }


job_manager = JobManager()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Re-attach to crawls that outlived a previous webapp process (Docker
    # restart aside, a plain `uvicorn` restart used to orphan them silently)
    # and start folding their output into progress state immediately, so the
    # admin page is accurate the moment it's opened rather than only after
    # the first poll.
    try:
        job_manager.reattach()
    except Exception:  # noqa: BLE001 - never block startup on job recovery
        LOG.exception("job re-attach failed")
    job_manager.start_background_pump()
    yield


app = FastAPI(title="wolt-il-search", lifespan=_lifespan)


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
    """Cache counts + per-category progress.

    `by_product_line` now carries the denominators the admin UI needs to draw
    progress (pending / retrying / gave_up) and counts venues that actually
    have items separately from ones merely *marked* fetched — those differ
    whenever a fetch succeeded but returned an empty catalog.
    """
    cache = _cache()
    try:
        counts = cache.counts()
        counts["by_product_line"] = cache.product_line_stats()
        counts["jobs"] = job_manager.snapshot()
        return JSONResponse(counts)
    finally:
        cache.close()


def _catalog(cache: Cache, refresh: bool = False) -> list:
    """Crawlable categories: live Wolt discovery merged with cache stats.

    Discovery is cached in `meta` for a day, so this is a local read unless
    the operator explicitly refreshes it.
    """
    client = WoltClient(delay_seconds=1.0)
    return build_catalog(cache, discover(client, cache, force=refresh))


@app.get("/api/categories")
def api_categories(refresh: bool = False) -> JSONResponse:
    cache = _cache()
    try:
        catalog = _catalog(cache, refresh=refresh)
        return JSONResponse({"categories": [c.to_json() for c in catalog]})
    finally:
        cache.close()


@app.get("/api/admin/state")
def api_admin_state() -> JSONResponse:
    """Everything the admin page renders, in one request.

    One poll for counts + categories + all job progress keeps the page's
    refresh cycle to a single round-trip, instead of the old page's
    status-poll plus one log fetch per open pane.
    """
    cache = _cache()
    try:
        catalog = _catalog(cache)
        counts = cache.counts()
        counts["by_product_line"] = cache.product_line_stats()
        return JSONResponse(
            {
                "counts": counts,
                "categories": [c.to_json() for c in catalog],
                "jobs": job_manager.snapshot(),
                "limits": {"max_concurrent": MAX_CONCURRENT_JOBS, "running": job_manager.running_count()},
            }
        )
    finally:
        cache.close()


@app.post("/api/jobs")
async def api_job_create(request: Request) -> JSONResponse:
    """Create one crawl job for a set of categories.

    Replaces the per-job `POST /api/jobs/{name}/start` endpoints: a job is now
    described by *what to crawl*, and defaults to crawling all of it.
    """
    body = await request.json() if await request.body() else {}
    cache = _cache()
    try:
        catalog = _catalog(cache)
        valid = {c.key for c in catalog}
        labels = {c.key: c.label for c in catalog}
        params = CrawlParams.from_request(body, valid)
        title = ", ".join(labels.get(k, k) for k in params.categories)
        if len(title) > 90:
            title = f"{len(params.categories)} categories"
        try:
            job = job_manager.start("crawl", title, params.to_argv(), dataclasses.asdict(params))
        except RuntimeError as e:
            raise HTTPException(409, str(e)) from e
        # Remember the advanced settings so the form comes back the way the
        # operator left it, without re-typing.
        cache.set_meta(
            "crawl_defaults",
            json.dumps(
                {
                    "limit": params.limit,
                    "max_age_hours": params.max_age_hours,
                    "delay": params.delay,
                    "max_delay": params.max_delay,
                    "max_attempts": params.max_attempts,
                    "phases": list(params.phases),
                }
            ),
        )
        return JSONResponse({"id": job.id, "job": job.to_json()})
    finally:
        cache.close()


@app.post("/api/jobs/reindex")
def api_job_reindex() -> JSONResponse:
    """Rebuild the FTS index without crawling (every crawl already does this)."""
    try:
        job = job_manager.start("reindex", "Rebuild search index", ["reindex"], {})
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return JSONResponse({"id": job.id})


@app.get("/api/jobs")
def api_jobs() -> JSONResponse:
    return JSONResponse({"jobs": job_manager.snapshot()})


@app.get("/api/jobs/defaults")
def api_job_defaults() -> JSONResponse:
    cache = _cache()
    try:
        raw = cache.get_meta("crawl_defaults")
        saved = json.loads(raw) if raw else {}
        return JSONResponse(
            {
                "limit": saved.get("limit", 0),
                "max_age_hours": saved.get("max_age_hours", 168.0),
                "delay": saved.get("delay", 1.2),
                "max_delay": saved.get("max_delay", 60.0),
                "max_attempts": saved.get("max_attempts", 4),
                "phases": saved.get("phases", ["discover", "items"]),
            }
        )
    finally:
        cache.close()


@app.post("/api/jobs/{job_id}/stop")
def api_job_stop(job_id: str) -> JSONResponse:
    try:
        return JSONResponse({"stopped": job_manager.stop(job_id)})
    except KeyError as e:
        raise HTTPException(404, f"unknown job: {job_id}") from e


@app.get("/api/jobs/{job_id}/log")
def api_job_log(job_id: str, after: int = 0) -> JSONResponse:
    """Incremental log tail: only lines newer than `after`.

    The old endpoint re-read the whole log file and returned the last N lines
    as one blob, which the page then wrote into a freshly-created <pre> —
    hence the flash and lost scroll position every 5 seconds.
    """
    try:
        return JSONResponse(job_manager.log_since(job_id, after))
    except KeyError as e:
        raise HTTPException(404, f"unknown job: {job_id}") from e


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
/* Form controls don't inherit the font stack on their own, so without this a
   <select> renders in the OS UI font while every label beside it doesn't. */
input, select, button, textarea { font-family: inherit; }
/* One checkbox appearance for the whole app. The shop facets need a custom
   box because they're tri-state (include / exclude / off), and a native
   checkbox can't express that — so rather than leave a 13px OS checkbox next
   to a 16px custom one in adjacent panels doing the same job, the native ones
   are restyled to match .facet-icon exactly. */
input[type="checkbox"] {
  appearance: none; -webkit-appearance: none;
  width: 1rem; height: 1rem; flex-shrink: 0; margin: 0;
  border: 1.5px solid var(--border); border-radius: 4px; background: none;
  display: inline-flex; align-items: center; justify-content: center; cursor: pointer;
}
input[type="checkbox"]:checked { background: var(--accent-soft); border-color: var(--accent); }
input[type="checkbox"]:checked::after {
  content: '\2713'; font-size: 0.7rem; font-weight: 700; line-height: 1; color: var(--accent);
}
input[type="checkbox"]:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
input[type="radio"] { accent-color: var(--accent); flex-shrink: 0; margin: 0; }
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
  /* --bg, not --surface: panels are --surface, so a same-colour input reads as
     a flat outline. Recessing it matches the admin form's inputs too. */
  width: 100%; padding: 0.5rem 0.6rem; border: 1px solid var(--border); border-radius: var(--radius-sm);
  font-size: 0.9rem; color: var(--text); background: var(--bg);
}
.field input:focus, .field select:focus { outline: 2px solid var(--accent); border-color: var(--accent); }
.range-row { display: flex; gap: 0.5rem; }

/* One primitive for every "toggle a filter" row, whatever widget it wraps:
   the region/category checklists, the shop include/exclude buttons, the
   standalone checkboxes in More filters, and the admin form's checkboxes.
   Each of those used to carry its own padding, gap, hover and focus
   treatment, so a single sidebar column read as four different control
   languages stacked on top of each other. */
.check-row, .field label.check-row, .checklist label, .facet-row {
  /* No width:100% — flex/grid parents already stretch these rows, and forcing
     it made a .check-row used inline in a toolbar claim the whole line. */
  display: flex; align-items: center; gap: 0.5rem; margin: 0;
  padding: 0.28rem 0.4rem; border-radius: var(--radius-sm);
  font-size: 0.85rem; font-weight: 400; line-height: 1.35; color: var(--text);
  text-transform: none; letter-spacing: 0;
  cursor: pointer; text-align: left; background: none; border: none;
}
.check-row:hover, .checklist label:hover, .facet-row:hover { background: var(--bg); }
.facet-row:hover .facet-label { color: var(--accent); }
/* focus-within so a keyboard user tabbing onto the nested checkbox sees the
   whole row highlighted, matching how the facet buttons behave. */
.check-row:focus-within, .checklist label:focus-within, .facet-row:focus-visible {
  outline: 2px solid var(--accent); outline-offset: -1px;
}
.checklist {
  max-height: 220px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.05rem;
  scrollbar-width: thin; scrollbar-color: var(--border) transparent;
  /* Reserve the gutter always, so counts never sit under the scrollbar thumb
     and a list doesn't reflow when it grows past the scroll threshold. */
  padding-right: 0.4rem; scrollbar-gutter: stable;
}
/* Counts right-align into a column rather than trailing the label text, so
   "which of these has the most venues" is a vertical scan. */
.checklist .count, .facet-count {
  margin-left: auto; padding-left: 0.6rem; color: var(--text-muted);
  font-size: 0.78rem; font-variant-numeric: tabular-nums;
}
/* Sits flush with the rows it clears (which now carry 0.4rem of side padding)
   instead of hanging off the panel's left edge. */
.chk-toggle {
  font-size: 0.75rem; color: var(--accent); cursor: pointer; text-decoration: underline; margin: 0 0 0.5rem 0.4rem;
  display: inline-block; background: none; border: none; padding: 0;
}
.chk-toggle:focus-visible { outline: 2px solid var(--accent); border-radius: 2px; }
.facet-hint { font-weight: 400; text-transform: none; letter-spacing: 0; font-size: 0.72rem; color: var(--text-muted); display: block; margin-top: 0.15rem; }
.facet-icon {
  width: 1rem; height: 1rem; border: 1.5px solid var(--border); border-radius: 4px; flex-shrink: 0;
  display: inline-flex; align-items: center; justify-content: center; font-size: 0.65rem; font-weight: 700; color: transparent;
}
.facet-icon.include { background: var(--green-soft); border-color: var(--green); color: var(--green); }
.facet-icon.exclude { background: var(--red-soft); border-color: var(--red); color: var(--red); }
/* Truncate rather than wrap: a wrapped label made rows different heights and
   pushed its count out of the column. */
.facet-label, .chk-label { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

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


#: Helpers both pages need. Kept in one string for the same reason as
#: _BASE_CSS: the two pages are separate documents with no bundler, so
#: anything not shared here silently drifts apart between them.
_BASE_JS = r"""
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

const fmtInt = (n) => (n ?? 0).toLocaleString();

function debounce(fn, ms) {
  let handle;
  return (...args) => { clearTimeout(handle); handle = setTimeout(() => fn(...args), ms); };
}

// Only write when the value actually changed, so repeated renders don't cause
// needless layout/paint churn or clobber a text selection inside the node.
function setText(node, value) {
  const text = value == null ? '' : String(value);
  if (node.textContent !== text) node.textContent = text;
}
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
            <label class="check-row" for="onlyOpen"><input type="checkbox" id="onlyOpen"> Open now</label>
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

<script>""" + _BASE_JS + r"""
const PAGE_SIZE = 20;
let currentOffset = 0;
let currentTotal = 0;
// slug -> 'include' | 'exclude', persists across filter tweaks, cleared on a fresh text search
let shopFacetState = new Map();
// selected item-category facet values
let selectedCategories = new Set();
// Monotonic id of the newest search. Every render checks it before touching
// the DOM (see runSearch) so a slow early response can't overwrite a newer one.
let searchSeq = 0;
let inFlight = null;

function checklistHtml(id, items, valueKey, labelKey, countKey, humanize) {
  const el = document.getElementById(id);
  el.innerHTML = items.map(it => `
    <label><input type="checkbox" value="${esc(it[valueKey])}" data-group="${id}">
      <span class="chk-label" dir="auto">${esc(humanize ? humanizeLabel(it[labelKey]) : it[labelKey])}</span>
      ${countKey ? `<span class="count">${fmtInt(it[countKey])}</span>` : ''}
    </label>`).join('');
}

async function loadFilters() {
  // Fetched in parallel: three independent reads, no reason to serialize them.
  const [regions, plines, status] = await Promise.all([
    fetch('/api/regions').then(r => r.json()),
    fetch('/api/product-lines').then(r => r.json()),
    fetch('/api/status').then(r => r.json()),
  ]);
  checklistHtml('regionList', regions, 'slug', 'name', null, false);
  checklistHtml('plList', plines, 'product_line', 'product_line', 'count', true);
  setText(document.getElementById('headerStats'),
    `${fmtInt(status.venues)} venues · ${fmtInt(status.menu_items)} items indexed`);
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
  // Clicking a facet triggers a search, which re-renders this list — i.e. the
  // very button the user just activated gets destroyed under them, losing
  // keyboard focus and the scroll position. Remember both and put them back.
  const focusedSlug = document.activeElement?.closest?.('.facet-row')?.dataset.slug;
  const scrollTop = list.scrollTop;
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
  list.scrollTop = scrollTop;
  if (focusedSlug) list.querySelector(`.facet-row[data-slug="${CSS.escape(focusedSlug)}"]`)?.focus();
}

function renderCategoryFacets(facets) {
  const panel = document.getElementById('categoryFacetPanel');
  const list = document.getElementById('categoryFacetList');
  if (!facets.length) { panel.style.display = 'none'; return; }
  panel.style.display = 'block';
  const focusedValue = document.activeElement?.matches?.('input[data-category-facet]')
    ? document.activeElement.value : null;
  const scrollTop = list.scrollTop;
  list.innerHTML = facets.map(f => {
    const checked = selectedCategories.has(f.value) ? 'checked' : '';
    return `<label><input type="checkbox" value="${esc(f.value)}" ${checked} data-category-facet="1">
      <span class="chk-label" dir="auto">${esc(f.label)}</span> <span class="count">${fmtInt(f.count)}</span>
    </label>`;
  }).join('');
  list.scrollTop = scrollTop;
  if (focusedValue) {
    list.querySelector(`input[data-category-facet][value="${CSS.escape(focusedValue)}"]`)?.focus();
  }
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

// Every filter change calls doSearch(), and facet clicks aren't debounced, so
// several searches can legitimately be in flight at once. Without a guard the
// one that renders last wins — which is whichever the *server* happened to
// finish last, not the one the user asked for most recently.
async function runSearch(offset) {
  const seq = ++searchSeq;
  if (inFlight) inFlight.abort();  // don't make the server finish work nobody wants
  const controller = new AbortController();
  inFlight = controller;
  try {
    const resp = await fetch('/api/search?' + buildParams(offset), { signal: controller.signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    return seq === searchSeq ? data : null;  // stale: a newer search already started
  } finally {
    if (inFlight === controller) inFlight = null;
  }
}

function searchErrorHtml(err) {
  return `<div class="empty-state">Search failed: ${esc(err.message || err)}.
    <button type="button" id="retrySearch" class="more-items" style="margin-top:.6rem">Try again</button></div>`;
}

// --- shareable URLs -------------------------------------------------------
// buildParams() already produces the exact querystring the API takes, so the
// address bar can just mirror it (minus pagination). Without this a search
// can't be bookmarked or shared, and a reload silently drops the query,
// facets, price range and sort.
const URL_SKIP = new Set(['offset', 'limit', 'items_per_venue']);

function syncUrl(newSearch) {
  const params = buildParams(0);
  URL_SKIP.forEach((k) => params.delete(k));
  if (!params.get('q')) params.delete('q');
  const qs = params.toString();
  const url = qs ? `${location.pathname}?${qs}` : location.pathname;
  if (url === location.pathname + location.search) return;
  // Submitting a new query gets a history entry, so Back returns to the
  // previous search. Tweaking a filter or a facet only rewrites the current
  // entry — those fire on debounced input, and one entry per keystroke would
  // make Back useless for anything else.
  if (newSearch) history.pushState(null, '', url);
  else history.replaceState(null, '', url);
}

function restoreFromUrl() {
  const p = new URLSearchParams(location.search);
  if (![...p.keys()].length) return false;
  const setValue = (id, key) => { const v = p.get(key); if (v != null) document.getElementById(id).value = v; };
  setValue('q', 'q');
  setValue('minRating', 'min_rating');
  setValue('minPrice', 'min_price');
  setValue('maxPrice', 'max_price');
  setValue('sort', 'sort');
  document.getElementById('onlyOpen').checked = p.get('open') === 'true';
  const check = (group, key) => {
    const wanted = new Set((p.get(key) || '').split(',').filter(Boolean));
    document.querySelectorAll(`input[data-group="${group}"]`).forEach((cb) => { cb.checked = wanted.has(cb.value); });
  };
  check('regionList', 'regions');
  check('plList', 'product_lines');
  (p.get('include_venues') || '').split(',').filter(Boolean).forEach((s) => shopFacetState.set(s, 'include'));
  (p.get('exclude_venues') || '').split(',').filter(Boolean).forEach((s) => shopFacetState.set(s, 'exclude'));
  (p.get('categories') || '').split(',').filter(Boolean).forEach((c) => selectedCategories.add(c));
  return true;
}

async function doSearch(opts) {
  if (opts && opts.resetFacetState) { shopFacetState.clear(); selectedCategories.clear(); }
  const q = document.getElementById('q').value.trim();
  const results = document.getElementById('results');
  const meta = document.getElementById('resultMeta');
  const loadMore = document.getElementById('loadMore');
  currentOffset = 0;
  syncUrl(opts && opts.resetFacetState);
  if (!q) {
    results.innerHTML = ''; meta.textContent = ''; loadMore.style.display = 'none';
    renderShopFacets([]); renderCategoryFacets([]);
    return;
  }

  results.innerHTML = '<div class="loading-state">Searching...</div>';
  meta.textContent = '';
  loadMore.style.display = 'none';

  let data;
  try {
    data = await runSearch(0);
  } catch (err) {
    if (err.name === 'AbortError') return;  // superseded on purpose, not a failure
    results.innerHTML = searchErrorHtml(err);
    document.getElementById('retrySearch')?.addEventListener('click', () => doSearch());
    return;
  }
  if (!data) return;  // a newer search is already rendering

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
    // Deliberately a plain fetch, not runSearch(): appending a page must not
    // abort or be aborted by the primary search, and it has no stale-render
    // problem because it only ever appends at the current offset.
    const resp = await fetch('/api/search?' + buildParams(currentOffset));
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    const results = document.getElementById('results');
    results.insertAdjacentHTML('beforeend', data.results.map(venueCardHtml).join(''));
    currentOffset += data.results.length;
    document.getElementById('resultMeta').textContent = `Showing ${currentOffset} of ${currentTotal.toLocaleString()} matching venues`;
    btn.style.display = currentOffset < currentTotal ? 'block' : 'none';
  } catch (err) {
    document.getElementById('resultMeta').textContent = `Couldn't load more: ${err.message || err}`;
  } finally {
    btn.disabled = false;
    btn.textContent = 'Load more';
  }
}

const debouncedSearch = debounce(() => doSearch(), 400);

document.getElementById('go').addEventListener('click', () => doSearch({ resetFacetState: true }));
document.getElementById('loadMore').addEventListener('click', loadMoreResults);
document.getElementById('q').addEventListener('keydown', e => { if (e.key === 'Enter') doSearch({ resetFacetState: true }); });
['regionList', 'plList'].forEach(id => document.getElementById(id).addEventListener('change', () => doSearch()));
['onlyOpen', 'sort'].forEach(id => document.getElementById(id).addEventListener('change', () => doSearch()));
['minRating', 'minPrice', 'maxPrice'].forEach(id => document.getElementById(id).addEventListener('input', debouncedSearch));

// Back/forward should reload the search that URL describes, not leave the page
// showing results from a different query.
window.addEventListener('popstate', () => {
  shopFacetState.clear();
  selectedCategories.clear();
  restoreFromUrl();  // may be an empty URL: that's a legitimate "blank page" state
  doSearch();
});

(async function init() {
  try {
    await loadFilters();
  } catch (err) {
    // A bare loadFilters() call used to swallow this: the filter checklists
    // would just silently stay empty with no hint that anything had failed.
    document.getElementById('results').innerHTML =
      `<div class="empty-state">Couldn't load filters: ${esc(err.message || err)}.
         Is the server still running? <button type="button" id="retryBoot" class="more-items"
         style="margin-top:.6rem">Retry</button></div>`;
    document.getElementById('retryBoot')?.addEventListener('click', () => location.reload());
    return;
  }
  // Restore only after the checklists exist — region/product-line boxes are
  // built from the API response above, so there's nothing to tick before this.
  if (restoreFromUrl()) doSearch();
})();
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
  .layout { grid-template-columns: 1fr; max-width: 1040px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.5rem 0.7rem; border-bottom: 1px solid var(--border); font-size: 0.86rem; }
  th { color: var(--text-muted); font-weight: 600; font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.03em; }
  tr:last-child td { border-bottom: none; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }

  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 0.8rem; }
  .stat { background: var(--bg); border-radius: var(--radius-sm); padding: 0.6rem 0.75rem; }
  .stat .k { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); }
  .stat .v { font-size: 1.25rem; font-weight: 700; font-variant-numeric: tabular-nums; margin-top: 0.15rem; }
  .stat.warn .v { color: var(--red); }

  button.primary:disabled { opacity: 0.5; cursor: not-allowed; }
  button.ghost {
    background: none; color: var(--text-muted); border: 1px solid var(--border); border-radius: 999px;
    padding: 0.4rem 0.9rem; font-size: 0.82rem; font-weight: 600; cursor: pointer; font-family: inherit;
  }
  button.ghost:hover { color: var(--accent); border-color: var(--accent); }
  button.stop { background: var(--red); color: var(--bg); border: none; border-radius: 999px; padding: 0.35rem 0.9rem; cursor: pointer; font-weight: 700; }

  /* ---- category picker ---- */
  .cat-toolbar { display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap; margin-bottom: 0.7rem; }
  .cat-toolbar input[type="search"] {
    flex: 1 1 180px; min-width: 150px; padding: 0.45rem 0.7rem; border: 1px solid var(--border);
    border-radius: var(--radius-sm); background: var(--bg); color: var(--text); font-size: 0.88rem; font-family: inherit;
  }
  .cat-group-title {
    font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted);
    font-weight: 700; margin: 0.9rem 0 0.35rem;
  }
  .cat-group-title:first-child { margin-top: 0; }
  .cat-list { display: grid; grid-template-columns: repeat(auto-fill, minmax(310px, 1fr)); gap: 0.25rem 1rem; }
  /* Layout only — the row's padding, gap, hover and focus come from the
     shared .check-row primitive in _BASE_CSS, so a category row here and a
     region row on the search page are the same control. */
  .cat-row .cat-label { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cat-row .cat-meta { margin-left: auto; padding-left: 0.5rem; font-size: 0.74rem; color: var(--text-muted); font-variant-numeric: tabular-nums; white-space: nowrap; }
  .cat-row .cat-meta b { color: var(--accent); font-weight: 700; }
  .pill {
    font-size: 0.66rem; font-weight: 700; letter-spacing: 0.02em; padding: 0.05rem 0.4rem;
    border-radius: 999px; background: var(--accent-soft); color: var(--accent); text-transform: uppercase;
  }
  .selection-summary { font-size: 0.85rem; color: var(--text-muted); margin: 0.9rem 0 0.2rem; }
  .selection-summary b { color: var(--text); font-variant-numeric: tabular-nums; }

  details.advanced { margin-top: 0.9rem; border-top: 1px solid var(--border); padding-top: 0.7rem; }
  details.advanced > summary {
    cursor: pointer; font-size: 0.82rem; font-weight: 700; color: var(--accent); list-style: none;
    display: flex; align-items: center; gap: 0.35rem;
  }
  details.advanced > summary::-webkit-details-marker { display: none; }
  details.advanced > summary::before { content: '▸'; font-size: 0.75rem; }
  details.advanced[open] > summary::before { content: '▾'; }
  .adv-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 0.8rem; margin-top: 0.8rem; }
  .adv-grid .field label { display: flex; align-items: center; gap: 0.25rem; }
  .adv-note { font-size: 0.74rem; color: var(--text-muted); margin-top: 0.15rem; }

  .start-row { display: flex; align-items: center; gap: 0.9rem; margin-top: 1.1rem; flex-wrap: wrap; }
  .start-hint { font-size: 0.8rem; color: var(--text-muted); }

  /* ---- job cards ---- */
  .job-card { background: var(--bg); border-radius: var(--radius); padding: 0.85rem 1rem; margin-bottom: 0.7rem; }
  .job-card:last-child { margin-bottom: 0; }
  .job-top { display: flex; align-items: flex-start; gap: 0.8rem; }
  .job-title { font-weight: 700; font-size: 0.95rem; flex: 1; min-width: 0; }
  .job-sub { color: var(--text-muted); font-size: 0.78rem; margin-top: 0.1rem; }
  .dot-status { display: inline-block; width: 0.55rem; height: 0.55rem; border-radius: 50%; margin-right: 0.4rem; flex-shrink: 0; }
  .dot-status.running { background: var(--green); animation: pulse 1.6s ease-in-out infinite; }
  .dot-status.idle { background: var(--text-muted); }
  .dot-status.done { background: var(--accent); }
  .dot-status.failed, .dot-status.orphaned { background: var(--red); }
  .dot-status.stopped { background: #d9a13b; }
  @keyframes pulse { 50% { opacity: 0.35; } }
  .job-state { font-size: 0.8rem; color: var(--text-muted); white-space: nowrap; }

  .phase-line { display: flex; align-items: baseline; gap: 0.5rem; margin-top: 0.7rem; font-size: 0.85rem; }
  .phase-name { font-weight: 600; }
  .phase-step { color: var(--text-muted); font-size: 0.76rem; }
  .bar { height: 0.5rem; background: var(--border); border-radius: 999px; overflow: hidden; margin-top: 0.4rem; }
  .bar > i { display: block; height: 100%; width: 0; background: var(--accent-fill); border-radius: 999px; transition: width 0.35s ease; }
  .bar.indeterminate > i { width: 35% !important; animation: slide 1.4s ease-in-out infinite; }
  @keyframes slide { 0% { margin-left: -35%; } 100% { margin-left: 100%; } }
  .counters { display: flex; flex-wrap: wrap; gap: 0.15rem 1rem; margin-top: 0.45rem; font-size: 0.78rem; color: var(--text-muted); font-variant-numeric: tabular-nums; }
  .counters b { color: var(--text); font-weight: 600; }
  .counters .warn b { color: var(--red); }
  .current-line {
    margin-top: 0.4rem; font-size: 0.8rem; color: var(--text-muted);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .current-line code { color: var(--accent); background: var(--accent-soft); padding: 0.05rem 0.35rem; border-radius: 4px; }
  .backoff-note { margin-top: 0.4rem; font-size: 0.78rem; color: #f0c674; }
  .job-actions { display: flex; align-items: center; gap: 0.5rem; margin-top: 0.7rem; }

  pre.joblog {
    background: #05070f; color: #d1d5db; padding: 0.6rem 0.75rem; border-radius: var(--radius-sm);
    max-height: 300px; overflow: auto; font-size: 0.76rem; line-height: 1.5; margin: 0.6rem 0 0;
    white-space: pre-wrap; word-break: break-word; display: none;
  }
  pre.joblog.shown { display: block; }
  .empty-note { color: var(--text-muted); font-size: 0.87rem; padding: 0.4rem 0; }

  .info-icon {
    display: inline-flex; align-items: center; justify-content: center;
    width: 0.95rem; height: 0.95rem; border-radius: 50%; position: relative;
    background: var(--border); color: var(--text-muted); font-size: 0.65rem; font-weight: 700;
    cursor: help; flex-shrink: 0;
  }
  .info-icon:hover::after, .info-icon:focus-visible::after {
    content: attr(data-tip); position: absolute; bottom: 135%; left: 50%; transform: translateX(-50%);
    background: var(--text); color: var(--bg); padding: 0.45rem 0.65rem; border-radius: 6px;
    font-size: 0.72rem; font-weight: 400; white-space: normal; width: 16rem; text-transform: none;
    letter-spacing: 0; box-shadow: var(--shadow); z-index: 20; pointer-events: none;
  }
  .info-icon:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">Wolt IL <span class="dot">Search</span></div>
  <div style="display:flex; align-items:center; gap:1rem;">
    <span class="stats" id="headerStats"></span>
    <nav><a href="/">&larr; Search</a></nav>
  </div>
</div>
<div class="layout">

  <div class="panel">
    <div class="panel-header">New crawl job</div>
    <div class="panel-body">
      <div class="cat-toolbar">
        <input type="search" id="catFilter" placeholder="Filter categories…" autocomplete="off">
        <button type="button" class="ghost" id="selPending">Select all with pending</button>
        <button type="button" class="ghost" id="selClear">Clear</button>
        <label class="check-row"><input type="checkbox" id="showCurated"> show curated &amp; cuisine lists</label>
        <button type="button" class="ghost" id="refreshCats" title="Re-read Wolt's front page for new verticals/categories">↻ Wolt</button>
      </div>
      <div id="catList"></div>

      <div class="selection-summary" id="selectionSummary"></div>

      <details class="advanced" id="advanced">
        <summary>Advanced settings</summary>
        <div class="adv-grid">
          <div class="field">
            <label for="advPhases">Phases <span class="info-icon" tabindex="0" data-tip="Discover = re-list which venues exist in each region. Items = fetch each venue's menu/catalog. Both is the default.">ⓘ</span></label>
            <select id="advPhases">
              <option value="discover,items">Discover + items</option>
              <option value="items">Items only</option>
              <option value="discover">Discover only</option>
            </select>
          </div>
          <div class="field">
            <label for="advLimit">Max venues <span class="info-icon" tabindex="0" data-tip="Leave at 0 to crawl the entire selection, which is the default. Set a number only when you want a small test batch.">ⓘ</span></label>
            <input type="number" id="advLimit" min="0" step="10" value="0">
            <div class="adv-note">0 = whole category</div>
          </div>
          <div class="field">
            <label for="advMaxAge">Re-fetch older than (h) <span class="info-icon" tabindex="0" data-tip="Venues fetched more recently than this are skipped. Lower catches price/availability changes sooner; higher skips anything crawled recently.">ⓘ</span></label>
            <input type="number" id="advMaxAge" min="0" step="1" value="168">
          </div>
          <div class="field">
            <label for="advDelay">Base delay (s) <span class="info-icon" tabindex="0" data-tip="Minimum gap between requests to Wolt. This is a floor, not a fixed value — the crawler widens the gap automatically while Wolt rate-limits and recovers afterwards.">ⓘ</span></label>
            <input type="number" id="advDelay" min="0.5" max="30" step="0.1" value="1.2">
          </div>
          <div class="field">
            <label for="advMaxDelay">Backoff ceiling (s) <span class="info-icon" tabindex="0" data-tip="Upper bound for exponential backoff between retries and for the adaptive pacing.">ⓘ</span></label>
            <input type="number" id="advMaxDelay" min="5" max="300" step="5" value="60">
          </div>
          <div class="field">
            <label for="advAttempts">Attempts per request <span class="info-icon" tabindex="0" data-tip="Tries per HTTP request before the venue is recorded as failed and retried in a later run. Waits grow exponentially (2s, 4s, 8s…) with jitter, and honor Retry-After.">ⓘ</span></label>
            <input type="number" id="advAttempts" min="1" max="8" step="1" value="4">
          </div>
          <div class="field">
            <label class="check-row" for="advForce">
              <input type="checkbox" id="advForce">
              Force re-crawl
              <span class="info-icon" tabindex="0" data-tip="Ignore freshness entirely and re-fetch every venue in the selection, even ones crawled minutes ago.">ⓘ</span>
            </label>
          </div>
        </div>
      </details>

      <div class="start-row">
        <button class="primary" id="startBtn" disabled>Start crawl</button>
        <span class="start-hint" id="startHint"></span>
      </div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-header">Jobs</div>
    <div class="panel-body" id="jobList"></div>
  </div>

  <div class="panel">
    <div class="panel-header">Cache status</div>
    <div class="panel-body">
      <div class="stat-grid" id="statGrid"></div>
      <table style="margin-top:1rem">
        <thead><tr>
          <th>product line</th><th class="num">venues</th><th class="num">with items</th>
          <th class="num" title="Fetched successfully but hold zero items — the scrape fallback targets these">empty</th>
          <th class="num">pending</th><th class="num">retrying</th><th class="num">gave up</th>
        </tr></thead>
        <tbody id="plBody"></tbody>
      </table>
      <div class="start-row">
        <button type="button" class="ghost" id="reindexBtn">Rebuild search index</button>
        <span class="start-hint">Every crawl already reindexes on completion.</span>
      </div>
    </div>
  </div>
</div>

<script>""" + _BASE_JS + r"""
// ---------------------------------------------------------------------------
// Rendering rule for this page: build each node once, then patch it in place.
// The previous version reassigned innerHTML on the whole job container every
// 5s, which destroyed and recreated every input and every log pane — that's
// what made the page jitter, dropped focus while typing, and reset the log's
// scroll position on every poll.
// ---------------------------------------------------------------------------
const POLL_MS = 1500;
const els = {};              // cached static nodes
const jobCards = new Map();  // job id -> {root, refs...}
let categories = [];
let selected = new Set();
let openLogs = new Map();    // job id -> last seen line seq
let starting = false;

const $ = (id) => document.getElementById(id);

function fmtDuration(seconds) {
  if (seconds == null || !isFinite(seconds)) return '—';
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, '0')}s`;
  return `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, '0')}m`;
}
function fmtClock(ts) { return ts ? new Date(ts * 1000).toLocaleTimeString() : ''; }

// --- category picker -------------------------------------------------------
function catGroup(cat) {
  if (cat.kind === 'product_line') return 'Product lines (item crawls)';
  if (cat.kind === 'wolt_category') return 'Wolt cuisine/type categories';
  return cat.primary ? 'Main verticals' : 'Other Wolt verticals';
}
const GROUP_ORDER = [
  'Main verticals',
  'Product lines (item crawls)',
  'Other Wolt verticals',
  'Wolt cuisine/type categories',
];

function renderCategories() {
  const filter = els.catFilter.value.trim().toLowerCase();
  const showCurated = els.showCurated.checked;
  const groups = new Map(GROUP_ORDER.map((g) => [g, []]));

  categories.forEach((cat) => {
    const hideable = cat.curated || cat.kind === 'wolt_category';
    if (hideable && !showCurated && !selected.has(cat.key)) return;
    if (filter && !(`${cat.label} ${cat.key}`.toLowerCase().includes(filter))) return;
    groups.get(catGroup(cat))?.push(cat);
  });

  const frag = document.createDocumentFragment();
  GROUP_ORDER.forEach((name) => {
    const items = groups.get(name);
    if (!items || !items.length) return;
    const title = document.createElement('div');
    title.className = 'cat-group-title';
    title.textContent = `${name} (${items.length})`;
    frag.appendChild(title);
    const list = document.createElement('div');
    list.className = 'cat-list';
    items.forEach((cat) => list.appendChild(catRow(cat)));
    frag.appendChild(list);
  });
  if (!frag.childNodes.length) {
    const note = document.createElement('div');
    note.className = 'empty-note';
    note.textContent = 'No categories match that filter.';
    frag.appendChild(note);
  }
  // Whole-list rebuild is fine here: this only re-runs on explicit user
  // interaction (typing a filter, toggling a checkbox), never on a timer.
  els.catList.replaceChildren(frag);
}

function catRow(cat) {
  const row = document.createElement('label');
  row.className = 'check-row cat-row';
  const box = document.createElement('input');
  box.type = 'checkbox';
  box.checked = selected.has(cat.key);
  box.addEventListener('change', () => {
    if (box.checked) selected.add(cat.key); else selected.delete(cat.key);
    updateSelectionSummary();
  });
  const label = document.createElement('span');
  label.className = 'cat-label';
  label.textContent = cat.label;
  if (cat.source === 'wolt') {
    const pill = document.createElement('span');
    pill.className = 'pill';
    pill.textContent = 'wolt';
    pill.title = 'Discovered live from Wolt\'s front-page API';
    label.appendChild(document.createTextNode(' '));
    label.appendChild(pill);
  }
  const meta = document.createElement('span');
  meta.className = 'cat-meta';
  if (cat.venues) {
    meta.innerHTML = cat.pending
      ? `${fmtInt(cat.with_items)}/${fmtInt(cat.venues)} · <b>${fmtInt(cat.pending)} pending</b>`
      : `${fmtInt(cat.with_items)}/${fmtInt(cat.venues)}`;
  } else {
    meta.textContent = 'not crawled yet';
  }
  row.append(box, label, meta);
  return row;
}

function updateSelectionSummary() {
  const chosen = categories.filter((c) => selected.has(c.key));
  const pending = chosen.reduce((sum, c) => sum + (c.pending || 0), 0);
  const venues = chosen.reduce((sum, c) => sum + (c.venues || 0), 0);
  els.selectionSummary.innerHTML = chosen.length
    ? `Selected <b>${chosen.length}</b> categor${chosen.length === 1 ? 'y' : 'ies'} — about <b>${fmtInt(pending)}</b> venues still need items (of <b>${fmtInt(venues)}</b> known).`
    : 'Select one or more categories to crawl. The whole selection is crawled by default — no venue cap needed.';
  els.startBtn.disabled = !chosen.length || starting;
}

// --- job cards -------------------------------------------------------------
function buildJobCard(job) {
  const root = document.createElement('div');
  root.className = 'job-card';
  root.innerHTML = `
    <div class="job-top">
      <div style="flex:1; min-width:0">
        <div class="job-title"></div>
        <div class="job-sub"></div>
      </div>
      <div class="job-state"><span class="dot-status"></span><span class="state-label"></span></div>
    </div>
    <div class="phase-line">
      <span class="phase-name"></span><span class="phase-step"></span>
    </div>
    <div class="bar"><i></i></div>
    <div class="counters">
      <span>done <b class="c-done">0</b></span>
      <span>ok <b class="c-ok">0</b></span>
      <span class="warn">failed <b class="c-fail">0</b></span>
      <span>items <b class="c-items">0</b></span>
      <span>elapsed <b class="c-elapsed">—</b></span>
      <span>eta <b class="c-eta">—</b></span>
    </div>
    <div class="current-line"></div>
    <div class="backoff-note" hidden></div>
    <div class="job-actions">
      <button type="button" class="ghost toggle-log">show log</button>
      <button type="button" class="stop" hidden>Stop</button>
    </div>
    <pre class="joblog"></pre>
  `;
  const refs = {
    root,
    title: root.querySelector('.job-title'),
    sub: root.querySelector('.job-sub'),
    dot: root.querySelector('.dot-status'),
    stateLabel: root.querySelector('.state-label'),
    phaseName: root.querySelector('.phase-name'),
    phaseStep: root.querySelector('.phase-step'),
    bar: root.querySelector('.bar'),
    barFill: root.querySelector('.bar > i'),
    cDone: root.querySelector('.c-done'),
    cOk: root.querySelector('.c-ok'),
    cFail: root.querySelector('.c-fail'),
    cItems: root.querySelector('.c-items'),
    cElapsed: root.querySelector('.c-elapsed'),
    cEta: root.querySelector('.c-eta'),
    current: root.querySelector('.current-line'),
    backoff: root.querySelector('.backoff-note'),
    toggleLog: root.querySelector('.toggle-log'),
    stopBtn: root.querySelector('.stop'),
    log: root.querySelector('.joblog'),
  };
  refs.toggleLog.addEventListener('click', () => toggleLog(job.id, refs));
  refs.stopBtn.addEventListener('click', async () => {
    refs.stopBtn.disabled = true;
    refs.stopBtn.textContent = 'Stopping…';
    await fetch(`/api/jobs/${job.id}/stop`, { method: 'POST' });
    refresh();
  });
  return refs;
}

function setHidden(node, hidden) {
  if (node.hidden !== hidden) node.hidden = hidden;
}

function updateJobCard(refs, job) {
  const p = job.progress || {};
  const running = job.state === 'running';
  const elapsedTo = job.finished_at || Date.now() / 1000;

  setText(refs.title, job.title || job.kind);
  const params = job.params || {};
  const bits = [];
  if (params.phases) bits.push(params.phases.join(' + '));
  bits.push(params.limit ? `limit ${fmtInt(params.limit)}` : 'whole selection');
  if (params.force) bits.push('forced');
  if (params.delay) bits.push(`${params.delay}s base delay`);
  setText(refs.sub, `${bits.join(' · ')} — started ${fmtClock(job.created_at)}`);

  if (refs.dot.className !== `dot-status ${job.state}`) refs.dot.className = `dot-status ${job.state}`;
  let stateText = job.state;
  if (running) stateText = 'running';
  else if (job.state === 'done') stateText = 'finished';
  else if (job.state === 'failed') stateText = `failed (code ${job.returncode})`;
  else if (job.state === 'orphaned') stateText = 'lost (process gone)';
  setText(refs.stateLabel, stateText);

  // Phase / progress
  const phase = p.phase || (running ? 'starting…' : '—');
  setText(refs.phaseName, phase);
  setText(refs.phaseStep, p.phase_of ? `step ${p.phase_index}/${p.phase_of}` : '');

  const n = p.n || 0;
  const i = p.i || 0;
  // A finished job shows the bar full rather than whatever fraction its last
  // phase happened to reach — a trailing phase with nothing to do would
  // otherwise leave a completed crawl looking like it stalled at 0%.
  const pct = !running && job.state === 'done' ? 100 : (n > 0 ? Math.min(100, (i / n) * 100) : 0);
  const indeterminate = running && n === 0;
  refs.bar.classList.toggle('indeterminate', indeterminate);
  const width = indeterminate ? '35%' : `${pct}%`;
  if (refs.barFill.style.width !== width) refs.barFill.style.width = width;

  // While running, the counters describe the phase the bar is showing.
  // Once finished, they describe the whole job.
  const ok = running ? p.ok : (p.cum_ok || 0) + (p.ok || 0);
  const failed = running ? p.failed : (p.cum_failed || 0) + (p.failed || 0);
  const items = running ? p.items : (p.cum_items || 0) + (p.items || 0);
  setText(refs.cDone, running ? (n ? `${fmtInt(i)}/${fmtInt(n)}` : fmtInt(i)) : fmtInt(ok + failed));
  setText(refs.cOk, fmtInt(ok));
  setText(refs.cFail, fmtInt(failed));
  setText(refs.cItems, fmtInt(items));
  setText(refs.cElapsed, fmtDuration(elapsedTo - job.created_at));
  setText(refs.cEta, running ? fmtDuration(p.eta_seconds) : '—');

  // "What is it doing right now" — the question the old page couldn't answer.
  if (running && p.current) {
    refs.current.innerHTML = `working on <code></code>`;
    refs.current.querySelector('code').textContent = p.current;
  } else if (!running && p.summary) {
    const s = p.summary;
    setText(refs.current,
      `summary: ${fmtInt(s.venues_found)} venues found, ${fmtInt(s.items_succeeded)}/${fmtInt(s.items_attempted)} item fetches ok`);
  } else if (p.last_error) {
    setText(refs.current, `last error: ${p.last_error}`);
  } else {
    setText(refs.current, '');
  }

  // Backoff visibility: a crawl that's waiting should say so, not look frozen.
  const retry = p.last_retry;
  const recent = retry && (Date.now() / 1000 - (retry.at || 0) < 90);
  setHidden(refs.backoff, !(running && recent));
  if (running && recent) {
    setText(refs.backoff,
      `⏳ ${retry.reason} — backing off ${retry.wait}s (attempt ${retry.attempt}/${retry.of}, pacing now ${retry.pacing}s, ${p.retries} retries this run)`);
  }

  setHidden(refs.stopBtn, !running);
  if (!running && refs.stopBtn.disabled) { refs.stopBtn.disabled = false; refs.stopBtn.textContent = 'Stop'; }
}

async function toggleLog(jobId, refs) {
  if (openLogs.has(jobId)) {
    openLogs.delete(jobId);
    refs.log.classList.remove('shown');
    refs.toggleLog.textContent = 'show log';
    return;
  }
  openLogs.set(jobId, 0);
  refs.log.textContent = '';
  refs.log.classList.add('shown');
  refs.toggleLog.textContent = 'hide log';
  await pumpLog(jobId, refs);
}

async function pumpLog(jobId, refs) {
  const after = openLogs.get(jobId);
  if (after === undefined) return;
  let data;
  try {
    data = await (await fetch(`/api/jobs/${jobId}/log?after=${after}`)).json();
  } catch (e) { return; }
  openLogs.set(jobId, data.seq || after);
  if (!data.lines || !data.lines.length) {
    if (!refs.log.textContent) refs.log.textContent = '(no output yet)';
    return;
  }
  // Sticky auto-tail: only scroll if the operator was already at the bottom,
  // so reading back through history isn't yanked away on the next poll.
  const atBottom = refs.log.scrollHeight - refs.log.scrollTop - refs.log.clientHeight < 24;
  if (refs.log.textContent === '(no output yet)') refs.log.textContent = '';
  if (data.gap) refs.log.append('… (older lines dropped)\n');
  refs.log.append(data.lines.map((l) => l.text).join('\n') + '\n');
  if (atBottom) refs.log.scrollTop = refs.log.scrollHeight;
}

function renderJobs(jobs) {
  const seen = new Set();
  jobs.forEach((job, index) => {
    seen.add(job.id);
    let refs = jobCards.get(job.id);
    if (!refs) {
      refs = buildJobCard(job);
      jobCards.set(job.id, refs);
    }
    updateJobCard(refs, job);
    // Keep DOM order in sync with server order without touching innards.
    const currentAt = els.jobList.children[index];
    if (currentAt !== refs.root) els.jobList.insertBefore(refs.root, currentAt || null);
    if (openLogs.has(job.id)) pumpLog(job.id, refs);
  });
  jobCards.forEach((refs, id) => {
    if (!seen.has(id)) { refs.root.remove(); jobCards.delete(id); openLogs.delete(id); }
  });
  if (!jobs.length) {
    if (!els.jobList.querySelector('.empty-note')) {
      const note = document.createElement('div');
      note.className = 'empty-note';
      note.textContent = 'No jobs yet. Pick categories above and press Start crawl.';
      els.jobList.appendChild(note);
    }
  } else {
    els.jobList.querySelector('.empty-note')?.remove();
  }
}

// --- cache status ----------------------------------------------------------
function renderCounts(counts) {
  const stats = [
    ['Regions', counts.regions, false],
    ['Venues', counts.venues, false],
    ['With items', counts.venues_with_menu, false],
    ['Items', counts.menu_items, false],
    ['Pending', counts.venues_pending, false],
    ['Retrying', counts.venues_retrying, counts.venues_retrying > 0],
    ['Gave up', counts.venues_gave_up, counts.venues_gave_up > 0],
  ];
  if (!els.statGrid.children.length) {
    stats.forEach(([k]) => {
      const box = document.createElement('div');
      box.className = 'stat';
      box.innerHTML = `<div class="k"></div><div class="v"></div>`;
      box.querySelector('.k').textContent = k;
      els.statGrid.appendChild(box);
    });
  }
  stats.forEach(([, v, warn], idx) => {
    const box = els.statGrid.children[idx];
    setText(box.querySelector('.v'), fmtInt(v));
    box.classList.toggle('warn', !!warn);
  });
  setText(els.headerStats, `${fmtInt(counts.venues)} venues · ${fmtInt(counts.menu_items)} items`);

  const rows = counts.by_product_line || [];
  while (els.plBody.rows.length > rows.length) els.plBody.deleteRow(-1);
  rows.forEach((row, idx) => {
    let tr = els.plBody.rows[idx];
    if (!tr) {
      tr = els.plBody.insertRow();
      tr.innerHTML = '<td></td><td class="num"></td><td class="num"></td><td class="num"></td><td class="num"></td><td class="num"></td><td class="num"></td>';
    }
    const cells = tr.cells;
    setText(cells[0], row.product_line ?? '(none)');
    setText(cells[1], fmtInt(row.total));
    setText(cells[2], fmtInt(row.with_items));
    setText(cells[3], fmtInt(row.empty));
    setText(cells[4], fmtInt(row.pending));
    setText(cells[5], fmtInt(row.retrying));
    setText(cells[6], fmtInt(row.gave_up));
  });
}

// --- polling ---------------------------------------------------------------
let categoriesSignature = '';

async function refresh() {
  let state;
  try {
    state = await (await fetch('/api/admin/state')).json();
  } catch (e) { return; }

  renderCounts(state.counts);
  renderJobs(state.jobs || []);

  // Only re-render the picker when the category data actually changed —
  // otherwise a poll would blow away a half-typed filter or a checkbox the
  // operator just clicked.
  const signature = JSON.stringify((state.categories || []).map((c) => [c.key, c.venues, c.with_items, c.pending]));
  if (signature !== categoriesSignature) {
    categoriesSignature = signature;
    categories = state.categories || [];
    renderCategories();
    updateSelectionSummary();
  }

  const limits = state.limits || {};
  const full = (limits.running || 0) >= (limits.max_concurrent || 3);
  els.startHint.textContent = full
    ? `${limits.running}/${limits.max_concurrent} jobs running — stop one before starting another.`
    : (selected.size ? '' : '');
  if (full) els.startBtn.disabled = true; else updateSelectionSummary();
}

async function startCrawl() {
  starting = true;
  els.startBtn.disabled = true;
  els.startBtn.textContent = 'Starting…';
  const body = {
    categories: [...selected],
    phases: els.advPhases.value.split(','),
    limit: Number(els.advLimit.value || 0),
    max_age_hours: Number(els.advMaxAge.value || 168),
    delay: Number(els.advDelay.value || 1.2),
    max_delay: Number(els.advMaxDelay.value || 60),
    max_attempts: Number(els.advAttempts.value || 4),
    force: els.advForce.checked,
  };
  try {
    const resp = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      els.startHint.textContent = err.detail || `Failed to start (HTTP ${resp.status})`;
    }
  } finally {
    starting = false;
    els.startBtn.textContent = 'Start crawl';
    await refresh();
  }
}

(async function init() {
  ['catFilter', 'showCurated', 'catList', 'selectionSummary', 'startBtn', 'startHint', 'jobList',
   'statGrid', 'plBody', 'headerStats', 'advPhases', 'advLimit', 'advMaxAge', 'advDelay',
   'advMaxDelay', 'advAttempts', 'advForce'].forEach((id) => { els[id] = $(id); });

  els.catFilter.addEventListener('input', renderCategories);
  els.showCurated.addEventListener('change', renderCategories);
  els.startBtn.addEventListener('click', startCrawl);
  $('selClear').addEventListener('click', () => { selected.clear(); renderCategories(); updateSelectionSummary(); });
  $('selPending').addEventListener('click', () => {
    categories.filter((c) => c.kind === 'product_line' && c.pending > 0).forEach((c) => selected.add(c.key));
    renderCategories();
    updateSelectionSummary();
  });
  $('refreshCats').addEventListener('click', async (e) => {
    e.target.disabled = true;
    e.target.textContent = '↻ …';
    try {
      const data = await (await fetch('/api/categories?refresh=true')).json();
      categories = data.categories || [];
      categoriesSignature = '';
      renderCategories();
      updateSelectionSummary();
    } finally {
      e.target.disabled = false;
      e.target.textContent = '↻ Wolt';
    }
  });
  $('reindexBtn').addEventListener('click', async (e) => {
    e.target.disabled = true;
    await fetch('/api/jobs/reindex', { method: 'POST' });
    e.target.disabled = false;
    refresh();
  });

  // Restore the advanced settings the operator used last time.
  try {
    const d = await (await fetch('/api/jobs/defaults')).json();
    els.advLimit.value = d.limit ?? 0;
    els.advMaxAge.value = d.max_age_hours ?? 168;
    els.advDelay.value = d.delay ?? 1.2;
    els.advMaxDelay.value = d.max_delay ?? 60;
    els.advAttempts.value = d.max_attempts ?? 4;
    if (d.phases) els.advPhases.value = d.phases.join(',');
  } catch (e) { /* defaults already in the markup */ }

  await refresh();
  setInterval(refresh, POLL_MS);
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
