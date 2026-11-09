"""Observability: structured JSON logging, request correlation, Prometheus metrics."""
import contextvars
import json
import logging
import sys
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from . import config

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")

# ---------------------------------------------------------------- metrics
HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests", ["service", "route", "method", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["service", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
DOWNSTREAM_CALLS = Counter("downstream_calls_total", "Calls to dependencies", ["service", "target", "outcome"])
RETRIES = Counter("downstream_retries_total", "Retry attempts", ["service", "target"])
CB_STATE = Gauge("circuit_breaker_state", "0=closed 1=half-open 2=open", ["service", "target"])
CB_OPENS = Counter("circuit_breaker_open_total", "Transitions to OPEN", ["service", "target"])
ENROLLMENTS = Counter("enrollments_total", "Enrollment outcomes", ["outcome"])
RECOVERY_ACTIONS = Counter("recovery_actions_total", "Background recovery actions", ["service", "action"])
SECURITY_EVENTS = Counter("security_events_total", "Denied/suspicious operations", ["service", "event"])
UP = Gauge("service_up", "Service process is running", ["service", "instance"])


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": config.SERVICE_NAME,
            "instance": config.INSTANCE_ID,
            "request_id": request_id_var.get(),
            "msg": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info).splitlines()[-1]
        return json.dumps(payload, default=str)


def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log(logger: logging.Logger, level: int, msg: str, **fields):
    logger.log(level, msg, extra={"fields": fields})


def install(app: FastAPI, service: str):
    """Request-ID propagation, access log, latency/status metrics, /metrics."""
    logger = get_logger("access")
    UP.labels(service, config.INSTANCE_ID).set(1)
    # Pre-create counter series at 0 so that Prometheus increase()/rate() also
    # count the very first event after a (re)start (a series that appears with
    # value 1 has no earlier sample to compare against).
    for ev in ("missing_token", "expired_token", "invalid_token", "forbidden_role", "internal_endpoint_denied",
               "login_failed", "login_locked", "oversized_payload", "idor_denied", "internal_via_gateway",
               "rate_limited"):
        SECURITY_EVENTS.labels(service, ev)
    for tgt in ("course", "notification"):
        RETRIES.labels(service, tgt)
        CB_OPENS.labels(service, tgt)
    for act in ("pending_completed", "outbox_sent"):
        RECOVERY_ACTIONS.labels(service, act)

    @app.middleware("http")
    async def _obs(request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        token = request_id_var.set(rid)
        route = request.scope.get("path", "")
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            r = request.scope.get("route")
            route = getattr(request.state, "route_label", None) or getattr(r, "path", None) or "unmatched"
            response.headers["x-request-id"] = rid
            response.headers.setdefault("x-served-by", f"{service}-{config.INSTANCE_ID}")
            return response
        finally:
            dur = time.perf_counter() - start
            if route not in ("/metrics", "/health/live", "/health/ready", "/ui/status", "/"):
                HTTP_REQUESTS.labels(service, route, request.method, str(status)).inc()
                HTTP_LATENCY.labels(service, route).observe(dur)
                log(logger, logging.INFO, "request", method=request.method, route=route,
                    status=status, duration_ms=round(dur * 1000, 1))
            request_id_var.reset(token)

    @app.get("/metrics", include_in_schema=False)
    async def _metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
