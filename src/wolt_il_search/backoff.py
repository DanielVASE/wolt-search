"""Shared HTTP retry/backoff + adaptive throttling for the Wolt crawlers.

Both `WoltClient` (JSON API) and `RetailScraper` (HTML pages) used to carry
their own byte-for-byte identical "sleep a fixed delay, retry a 429 once
after ~5s, otherwise give up" logic. That wasn't backoff at all:

* a single retry with a *constant* wait, so a rate-limit that lasts longer
  than one sleep is indistinguishable from a permanent failure;
* no retry whatsoever for 5xx / timeouts / connection resets, which on an
  hours-long unattended crawl are routine;
* no `Retry-After` handling, so Wolt telling us exactly how long to wait was
  ignored;
* a fixed inter-request delay the operator had to hand-tune, with nothing
  reacting when the server started pushing back.

This module centralizes all of that:

`RetryPolicy`      — the knobs (baseline pacing, backoff ceiling, attempts).
`AdaptiveThrottle` — inter-request pacing that *widens* when the server
                     pushes back (multiplicative increase on 429/5xx) and
                     slowly recovers after sustained success (additive
                     decrease), i.e. AIMD. The crawl self-tunes instead of
                     relying on the operator picking a magic delay.
`request_with_retry` — exponential backoff with equal jitter, honoring
                     `Retry-After`, retrying only what's worth retrying and
                     failing fast (and *permanently*) on things like 404.
"""

from __future__ import annotations

import email.utils
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import requests

LOG = logging.getLogger("wolt_il_search.backoff")

# Worth retrying: rate limiting, request timeouts, and the transient/edge
# 5xx family. Everything else (401/403/404/410/422/...) is a fact about the
# request itself — retrying just burns the crawl's rate budget.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509, 520, 521, 522, 523, 524})

# A resource that's gone is gone: callers use this to stop re-queueing a
# venue forever (see indexer's permanent-vs-transient failure handling).
PERMANENT_STATUS = frozenset({400, 401, 403, 404, 405, 410, 422, 451})

# Even when Wolt asks for a long pause via Retry-After, don't let one header
# park a crawl for an hour.
MAX_RETRY_AFTER_SECONDS = 120.0


class HttpError(RuntimeError):
    """A request that failed after the retry policy was exhausted."""

    def __init__(self, message: str, *, status_code: int | None = None, permanent: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.permanent = permanent


@dataclass
class RetryPolicy:
    """Crawl pacing + retry configuration.

    base_delay:    baseline seconds between requests (the floor the adaptive
                   throttle recovers back down to).
    max_delay:     ceiling for both the adaptive delay and a backoff sleep.
    max_attempts:  total tries per request, so 4 = 1 try + 3 retries.
    backoff_start: first backoff window; deliberately larger than base_delay
                   because "we got throttled" needs a real pause, not
                   another baseline-sized gap.
    factor:        exponential growth per attempt.
    jitter:        additive randomness on baseline pacing, so a long crawl
                   doesn't settle into a perfectly periodic request pattern.
    """

    base_delay: float = 1.2
    max_delay: float = 60.0
    max_attempts: int = 4
    backoff_start: float = 2.0
    factor: float = 2.0
    jitter: float = 0.4
    adaptive: bool = True

    def backoff_seconds(self, attempt: int) -> float:
        """Equal-jitter exponential backoff for a 0-based attempt number.

        Half the window is fixed and half is random ("equal jitter"): full
        jitter can return a near-zero wait right after a 429, and no jitter
        makes concurrent crawlers retry in lockstep.
        """
        window = min(self.max_delay, self.backoff_start * (self.factor**attempt))
        return window / 2 + random.uniform(0, window / 2)


@dataclass
class AdaptiveThrottle:
    """Self-tuning inter-request pacing (AIMD).

    Kept per-client so the JSON API and the HTML scraper back off
    independently — they're different hosts with different limits.
    """

    policy: RetryPolicy = field(default_factory=RetryPolicy)
    _delay: float = field(init=False)
    _last_request: float = field(default=0.0, init=False)
    _ok_streak: int = field(default=0, init=False)
    penalties: int = field(default=0, init=False)

    # Recover one step after this many consecutive clean responses.
    RECOVER_AFTER_OK = 25

    def __post_init__(self) -> None:
        self._delay = self.policy.base_delay

    @property
    def delay(self) -> float:
        return self._delay

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_request
        wait = self._delay - elapsed + random.uniform(0, self.policy.jitter)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def penalize(self) -> float:
        """Server pushed back — multiplicatively widen the baseline gap."""
        if not self.policy.adaptive:
            return self._delay
        self.penalties += 1
        self._ok_streak = 0
        self._delay = min(self.policy.max_delay, max(self._delay, self.policy.base_delay) * self.policy.factor)
        return self._delay

    def reward(self) -> None:
        """Sustained success — additively step the baseline gap back down."""
        if not self.policy.adaptive or self._delay <= self.policy.base_delay:
            self._ok_streak = 0
            return
        self._ok_streak += 1
        if self._ok_streak >= self.RECOVER_AFTER_OK:
            self._ok_streak = 0
            self._delay = max(self.policy.base_delay, self._delay - self.policy.base_delay)


def _retry_after_seconds(resp: requests.Response) -> float | None:
    """Parse `Retry-After`, which is legally either seconds or an HTTP date."""
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    raw = raw.strip()
    try:
        return max(0.0, min(float(raw), MAX_RETRY_AFTER_SECONDS))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    delta = when.timestamp() - time.time()
    return max(0.0, min(delta, MAX_RETRY_AFTER_SECONDS))


def request_with_retry(
    send: Callable[[], requests.Response],
    *,
    describe: str,
    policy: RetryPolicy,
    throttle: AdaptiveThrottle,
    on_retry: Callable[[dict], None] | None = None,
) -> requests.Response:
    """Run `send()` under baseline pacing + exponential backoff.

    `on_retry` receives a small dict per retry ({reason, status, attempt,
    wait, url}) so a crawl can surface "backing off 8.3s after HTTP 429"
    in its progress stream instead of looking frozen.
    """
    last_error: HttpError | None = None

    for attempt in range(policy.max_attempts):
        throttle.wait()
        status: int | None = None
        reason: str

        try:
            resp = send()
            status = resp.status_code
            if resp.ok:
                throttle.reward()
                return resp
            if status in PERMANENT_STATUS:
                # Don't spend retries — and tell the caller it's pointless
                # to re-queue this target at all.
                raise HttpError(
                    f"{describe} -> {status}: {resp.text[:200]}", status_code=status, permanent=True
                )
            if status not in RETRYABLE_STATUS:
                raise HttpError(f"{describe} -> {status}: {resp.text[:200]}", status_code=status)
            reason = f"HTTP {status}"
            wait = _retry_after_seconds(resp)
            last_error = HttpError(f"{describe} -> {status}: {resp.text[:200]}", status_code=status)
        except requests.exceptions.RequestException as e:
            # Timeouts / connection resets / chunked-encoding truncation are
            # exactly the failures a long crawl hits most, and previously got
            # zero retries.
            reason = type(e).__name__
            wait = None
            last_error = HttpError(f"{describe} -> {e}")

        widened = throttle.penalize()
        if attempt >= policy.max_attempts - 1:
            break

        sleep_for = wait if wait is not None else policy.backoff_seconds(attempt)
        if on_retry:
            on_retry(
                {
                    "reason": reason,
                    "status": status,
                    "attempt": attempt + 1,
                    "of": policy.max_attempts,
                    "wait": round(sleep_for, 2),
                    "pacing": round(widened, 2),
                    "target": describe,
                }
            )
        LOG.warning(
            "%s: %s — retry %d/%d in %.1fs (pacing now %.1fs)",
            describe,
            reason,
            attempt + 1,
            policy.max_attempts,
            sleep_for,
            widened,
        )
        time.sleep(sleep_for)

    assert last_error is not None  # loop always sets it before breaking
    raise last_error
