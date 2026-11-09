"""Unit tests for fault-tolerance and security primitives (no running services needed).

    pytest -q tests/
"""
import asyncio
import os
import sys
import time
from pathlib import Path

import httpx
import jwt
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("RESILIENCE_MODE", "improved")

from common import config  # noqa: E402
from common.resilience import (CLOSED, HALF_OPEN, OPEN, CircuitBreaker, CircuitOpenError,  # noqa: E402
                               ResilientClient, UpstreamError, backoff_delay)
from common.security import hash_password, issue_token, verify_password  # noqa: E402


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ----------------------------------------------------------- circuit breaker
def test_breaker_opens_after_threshold_and_blocks():
    cb = CircuitBreaker("x", failure_threshold=3, reset_timeout=5, clock=FakeClock(), enabled=True)
    for _ in range(2):
        assert cb.allow()
        cb.record_failure()
    assert cb.state == CLOSED
    cb.record_failure()
    assert cb.state == OPEN and cb.open_count == 1
    assert not cb.allow()


def test_breaker_half_open_single_trial_then_close():
    clk = FakeClock()
    cb = CircuitBreaker("x", failure_threshold=1, reset_timeout=5, clock=clk, enabled=True)
    cb.record_failure()
    assert cb.state == OPEN
    clk.t = 5.1
    assert cb.allow() and cb.state == HALF_OPEN
    assert not cb.allow(), "only one trial call in HALF_OPEN"
    cb.record_success()
    assert cb.state == CLOSED and cb.allow()


def test_breaker_half_open_failure_reopens():
    clk = FakeClock()
    cb = CircuitBreaker("x", failure_threshold=1, reset_timeout=5, clock=clk, enabled=True)
    cb.record_failure()
    clk.t = 6
    assert cb.allow()
    cb.record_failure()
    assert cb.state == OPEN and cb.open_count == 2


def test_breaker_refresh_moves_idle_open_breaker_to_half_open():
    clk = FakeClock()
    cb = CircuitBreaker("x", failure_threshold=1, reset_timeout=5, clock=clk, enabled=True)
    cb.record_failure()
    cb.refresh()
    assert cb.state == OPEN, "not before the reset timeout"
    clk.t = 5.1
    cb.refresh()
    assert cb.state == HALF_OPEN and cb.allow(), "trial call still available after refresh"


def test_breaker_disabled_never_blocks():
    cb = CircuitBreaker("x", failure_threshold=1, clock=FakeClock(), enabled=False)
    for _ in range(10):
        cb.record_failure()
        assert cb.allow()


# ----------------------------------------------------------- backoff
def test_backoff_is_bounded_and_exponential():
    assert backoff_delay(0, base=0.1, cap=1.0, rng=lambda: 1.0) == pytest.approx(0.1)
    assert backoff_delay(3, base=0.1, cap=1.0, rng=lambda: 1.0) == pytest.approx(0.8)
    assert backoff_delay(10, base=0.1, cap=1.0, rng=lambda: 1.0) == pytest.approx(1.0)  # capped
    assert backoff_delay(5, base=0.1, cap=1.0, rng=lambda: 0.0) == 0.0  # full jitter lower bound


# ----------------------------------------------------------- resilient client
def _client(handler, retries_enabled=True, retries=3):
    rc = ResilientClient("dep", ["http://a", "http://b"], timeout=0.5, retries=retries,
                         retries_enabled=retries_enabled)
    rc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rc.breaker = CircuitBreaker("dep", failure_threshold=50, enabled=True)
    return rc


def test_retry_masks_transient_failures_and_rotates_replicas(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_S", 0.001)
    seen = []

    def handler(req):
        seen.append(req.url.host)
        return httpx.Response(503) if len(seen) < 3 else httpx.Response(200, json={"ok": True})
    rc = _client(handler)
    r = asyncio.run(rc.request("GET", "/x", idempotent=True))
    assert r.status_code == 200 and len(seen) == 3
    assert seen[0] != seen[1], "client-side failover alternates replicas"


def test_retries_are_bounded(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_S", 0.001)
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(503)
    rc = _client(handler, retries=3)
    with pytest.raises(UpstreamError):
        asyncio.run(rc.request("GET", "/x", idempotent=True))
    assert len(calls) == 4  # 1 + 3 retries


def test_non_idempotent_calls_are_not_retried():
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(503)
    rc = _client(handler)
    with pytest.raises(UpstreamError):
        asyncio.run(rc.request("POST", "/x", idempotent=False))
    assert len(calls) == 1


def test_4xx_is_not_a_dependency_failure():
    rc = _client(lambda req: httpx.Response(409))
    r = asyncio.run(rc.request("POST", "/x", idempotent=True))
    assert r.status_code == 409 and rc.breaker.failures == 0


def test_timeout_is_enforced():
    def handler(req):
        raise httpx.ReadTimeout("slow", request=req)
    rc = _client(handler, retries_enabled=False)
    t0 = time.perf_counter()
    with pytest.raises(httpx.TimeoutException):
        asyncio.run(rc.request("GET", "/x", idempotent=True))
    assert time.perf_counter() - t0 < 1.0


def test_open_circuit_fails_fast_without_calling_dependency():
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(503)
    rc = _client(handler, retries_enabled=False)
    rc.breaker = CircuitBreaker("dep", failure_threshold=2, reset_timeout=60, enabled=True)
    for _ in range(2):
        with pytest.raises(UpstreamError):
            asyncio.run(rc.request("GET", "/x", idempotent=True))
    with pytest.raises(CircuitOpenError):
        asyncio.run(rc.request("GET", "/x", idempotent=True))
    assert len(calls) == 2


# ----------------------------------------------------------- security
def test_password_hash_roundtrip_and_salt():
    h1, h2 = hash_password("pw"), hash_password("pw")
    assert h1 != h2 and verify_password("pw", h1) and not verify_password("nope", h1)


def test_token_signature_and_expiry():
    tok = issue_token(7, "u", "student")
    claims = jwt.decode(tok, config.JWT_SECRET, algorithms=["HS256"])
    assert claims["sub"] == "7" and claims["role"] == "student"
    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(tok, "another-secret-of-sufficient-length-xx", algorithms=["HS256"])
    expired = issue_token(7, "u", "student", ttl=-10)
    with pytest.raises(jwt.ExpiredSignatureError):
        jwt.decode(expired, config.JWT_SECRET, algorithms=["HS256"])
