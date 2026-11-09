"""Software fault-tolerance primitives: timeouts, bounded retries with
exponential backoff + full jitter, circuit breaker, client-side failover."""
import asyncio
import logging
import random
import time
from typing import Sequence

import httpx

from . import config
from .obs import CB_OPENS, CB_STATE, DOWNSTREAM_CALLS, RETRIES, get_logger, log, request_id_var

logger = get_logger("resilience")

CLOSED, HALF_OPEN, OPEN = "closed", "half_open", "open"
_STATE_NUM = {CLOSED: 0, HALF_OPEN: 1, OPEN: 2}


class CircuitOpenError(Exception):
    pass


class UpstreamError(Exception):
    def __init__(self, status: int, body: str = ""):
        super().__init__(f"upstream status {status}")
        self.status = status
        self.body = body


class CircuitBreaker:
    """Consecutive-failure circuit breaker.

    CLOSED -> OPEN after `failure_threshold` consecutive failures.
    OPEN   -> HALF_OPEN after `reset_timeout` seconds (one trial call allowed).
    HALF_OPEN -> CLOSED on success, -> OPEN on failure.
    """

    def __init__(self, target: str, failure_threshold: int | None = None,
                 reset_timeout: float | None = None, clock=time.monotonic, enabled: bool | None = None):
        self.target = target
        self.failure_threshold = failure_threshold or config.CB_FAILURE_THRESHOLD
        self.reset_timeout = reset_timeout if reset_timeout is not None else config.CB_RESET_S
        self.enabled = config.FT_CIRCUIT_BREAKER if enabled is None else enabled
        self.clock = clock
        self.state = CLOSED
        self.failures = 0
        self.opened_at = 0.0
        self.open_count = 0
        self._trial_in_flight = False
        self._publish()

    def _publish(self):
        CB_STATE.labels(config.SERVICE_NAME, self.target).set(_STATE_NUM[self.state])

    def _transition(self, new: str):
        if new != self.state:
            log(logger, logging.WARNING, "circuit_breaker_transition", target=self.target,
                frm=self.state, to=new, failures=self.failures)
            self.state = new
            if new == OPEN:
                self.opened_at = self.clock()
                self.open_count += 1
                CB_OPENS.labels(config.SERVICE_NAME, self.target).inc()
            self._publish()

    def refresh(self):
        """Advance OPEN -> HALF_OPEN once the reset timeout has passed, without
        consuming the trial call. Called periodically so that the exported
        state does not stay 'open' on a replica that receives no traffic."""
        if self.enabled and self.state == OPEN and self.clock() - self.opened_at >= self.reset_timeout:
            self._transition(HALF_OPEN)
            self._trial_in_flight = False

    def allow(self) -> bool:
        if not self.enabled:
            return True
        if self.state == OPEN:
            if self.clock() - self.opened_at >= self.reset_timeout:
                self._transition(HALF_OPEN)
                self._trial_in_flight = False
            else:
                return False
        if self.state == HALF_OPEN:
            if self._trial_in_flight:
                return False
            self._trial_in_flight = True
        return True

    def record_success(self):
        self.failures = 0
        self._trial_in_flight = False
        if self.state != CLOSED:
            self._transition(CLOSED)

    def record_failure(self):
        self.failures += 1
        self._trial_in_flight = False
        if not self.enabled:
            return
        if self.state == HALF_OPEN or self.failures >= self.failure_threshold:
            self._transition(OPEN)


def backoff_delay(attempt: int, base: float | None = None, cap: float | None = None, rng=random.random) -> float:
    """Exponential backoff with full jitter: U(0, min(cap, base*2^attempt))."""
    base = config.RETRY_BASE_S if base is None else base
    cap = config.RETRY_CAP_S if cap is None else cap
    return rng() * min(cap, base * (2 ** attempt))


class ResilientClient:
    """HTTP client to one logical dependency with N replica URLs."""

    def __init__(self, target: str, urls: Sequence[str], timeout: float | None = None,
                 retries: int | None = None, retries_enabled: bool | None = None):
        self.target = target
        self.urls = list(urls)
        self.timeout = config.CALL_TIMEOUT_S if timeout is None else timeout
        self.retries_enabled = config.FT_RETRIES if retries_enabled is None else retries_enabled
        self.max_retries = config.RETRY_MAX if retries is None else retries
        self.breaker = CircuitBreaker(target)
        self._rr = 0
        self._client = httpx.AsyncClient(headers={"x-internal-token": config.INTERNAL_TOKEN})

    async def aclose(self):
        await self._client.aclose()

    def _next_url(self) -> str:
        url = self.urls[self._rr % len(self.urls)]
        self._rr += 1
        return url

    async def request(self, method: str, path: str, *, idempotent: bool, **kw) -> httpx.Response:
        """Returns the response for 2xx/4xx. Raises UpstreamError / httpx errors /
        CircuitOpenError when the dependency is considered failed."""
        attempts = 1 + (self.max_retries if (self.retries_enabled and idempotent) else 0)
        last_exc: Exception | None = None
        headers = kw.pop("headers", {}) or {}
        headers["x-request-id"] = request_id_var.get()
        for attempt in range(attempts):
            if not self.breaker.allow():
                DOWNSTREAM_CALLS.labels(config.SERVICE_NAME, self.target, "circuit_open").inc()
                raise CircuitOpenError(self.target)
            url = self._next_url()
            try:
                r = await self._client.request(method, url + path, timeout=self.timeout, headers=headers, **kw)
                if r.status_code >= 500:
                    raise UpstreamError(r.status_code, r.text[:200])
                self.breaker.record_success()
                DOWNSTREAM_CALLS.labels(config.SERVICE_NAME, self.target, "ok").inc()
                return r
            except (httpx.TransportError, UpstreamError) as exc:
                last_exc = exc
                self.breaker.record_failure()
                kind = "timeout" if isinstance(exc, httpx.TimeoutException) else type(exc).__name__
                DOWNSTREAM_CALLS.labels(config.SERVICE_NAME, self.target, kind).inc()
                if attempt < attempts - 1:
                    delay = backoff_delay(attempt)
                    RETRIES.labels(config.SERVICE_NAME, self.target).inc()
                    log(logger, logging.WARNING, "retrying", target=self.target, attempt=attempt + 1,
                        error=kind, delay_ms=round(delay * 1000))
                    await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc
