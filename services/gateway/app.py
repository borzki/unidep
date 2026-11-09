"""API gateway: routing, round-robin load balancing across replicas, active
health checks with ejection, failover to another replica for retry-safe
requests, per-principal rate limiting (token bucket)."""
import asyncio
import logging
import time
from pathlib import Path

import httpx
import jwt
from fastapi import Header, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from prometheus_client import Counter, Gauge

from common import config
from common.app_base import create_app
from common.obs import get_logger, log, request_id_var
from common.security import security_event

logger = get_logger("gateway")
FAILOVERS = Counter("gateway_failover_total", "Requests retried on another replica", ["upstream"])
UPSTREAM_HEALTHY = Gauge("gateway_upstream_healthy", "1 if replica passes readiness", ["upstream", "url"])
RATE_LIMITED = Counter("gateway_rate_limited_total", "Requests rejected by rate limiter", [])

ROUTES = [("/auth", "auth"), ("/courses", "course"), ("/enrollments", "enrollment"),
          ("/admin", "enrollment"), ("/notifications", "notification")]


class Pool:
    def __init__(self, name: str, urls: list[str]):
        self.name, self.urls = name, urls
        self.healthy = {u: True for u in urls}
        self.fails = {u: 0 for u in urls}
        self.rr = 0
        FAILOVERS.labels(name)
        for u in urls:
            UPSTREAM_HEALTHY.labels(name, u).set(1)

    def candidates(self) -> list[str]:
        n = len(self.urls)
        start = self.rr % n
        self.rr += 1
        ordered = [self.urls[(start + i) % n] for i in range(n)]
        if config.GW_HEALTH_CHECKS:
            healthy = [u for u in ordered if self.healthy[u]]
            return healthy or ordered  # if all look down, still try (fail open)
        return ordered

    def mark(self, url: str, ok: bool):
        if ok:
            self.fails[url] = 0
            if not self.healthy[url]:
                log(logger, logging.WARNING, "upstream_recovered", upstream=self.name, url=url)
            self.healthy[url] = True
        else:
            self.fails[url] += 1
            if self.fails[url] >= 2 and self.healthy[url]:
                log(logger, logging.WARNING, "upstream_ejected", upstream=self.name, url=url)
                self.healthy[url] = False
        UPSTREAM_HEALTHY.labels(self.name, url).set(1 if self.healthy[url] else 0)


def _parse_upstreams(spec: str) -> dict[str, Pool]:
    pools = {}
    for part in spec.split(";"):
        if part.strip():
            name, urls = part.split("=", 1)
            pools[name.strip()] = Pool(name.strip(), [u.strip() for u in urls.split(",") if u.strip()])
    return pools


pools = _parse_upstreams(config.UPSTREAMS)
client: httpx.AsyncClient | None = None
buckets: dict[str, list[float]] = {}  # key -> [tokens, last_ts]
_hc_task = None


async def _health_loop():
    async with httpx.AsyncClient(timeout=0.8) as hc:
        while True:
            for pool in pools.values():
                for u in pool.urls:
                    try:
                        r = await hc.get(u + "/health/ready")
                        pool.mark(u, r.status_code == 200)
                    except httpx.HTTPError:
                        pool.mark(u, False)
            await asyncio.sleep(config.GW_HEALTH_INTERVAL_S)


async def _start():
    global client, _hc_task
    client = httpx.AsyncClient(timeout=config.GW_UPSTREAM_TIMEOUT_S,
                               limits=httpx.Limits(max_connections=500, max_keepalive_connections=100))
    if config.GW_HEALTH_CHECKS:
        _hc_task = asyncio.create_task(_health_loop())


async def _stop():
    if _hc_task:
        _hc_task.cancel()
    await client.aclose()


def _ready_extra():
    return {f"info_{n}": sum(p.healthy.values()) for n, p in pools.items()}


app = create_app("gateway", None, on_start=_start, on_stop=_stop, extra_ready=_ready_extra)


UI_FILE = Path(__file__).parent / "static" / "index.html"


@app.get("/", include_in_schema=False)
async def ui_page():
    """Demo console (static page; all data comes from the normal API)."""
    return HTMLResponse(UI_FILE.read_text(encoding="utf-8"))


@app.get("/ui/status", include_in_schema=False)
async def ui_status():
    """Readiness of every replica, probed on demand (works in both profiles)."""
    async def probe(name, idx, url):
        try:
            r = await client.get(url + "/health/ready", timeout=0.6)
            j = r.json()
            return {"service": name, "index": idx, "up": True, "ready": r.status_code == 200, "checks": j.get("checks", {})}
        except Exception:  # noqa: BLE001
            return {"service": name, "index": idx, "up": False, "ready": False, "checks": {}}
    results = await asyncio.gather(*[probe(n, i, u) for n, p in pools.items() for i, u in enumerate(p.urls)])
    return {"mode": config.MODE, "replicas": list(results)}


@app.post("/ui/chaos", include_in_schema=False)
async def ui_chaos(request: Request, x_chaos_token: str | None = Header(default=None)):
    """Relays a fault-injection request to one replica. The replica itself
    checks the chaos token and only accepts it when CHAOS_ENABLED=1."""
    body = await request.json()
    service, idx = body.get("service"), int(body.get("index", 0))
    if service not in pools or not 0 <= idx < len(pools[service].urls):
        return JSONResponse({"detail": "unknown replica"}, status_code=404)
    url = pools[service].urls[idx]
    try:
        r = await client.post(url + "/chaos", json=body.get("state", {}), timeout=2.0,
                              headers={"x-chaos-token": x_chaos_token or ""})
    except httpx.HTTPError:
        return JSONResponse({"detail": "replica unreachable (already down?)"}, status_code=502)
    if r.status_code == 404:
        return JSONResponse({"detail": "chaos disabled on this replica (CHAOS_ENABLED=0)"}, status_code=404)
    log(logger, logging.WARNING, "ui_chaos", service=service, index=idx, state=body.get("state", {}), status=r.status_code)
    return JSONResponse(r.json() if r.headers.get("content-type", "").startswith("application/json") else {},
                        status_code=r.status_code)


def _principal_key(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        try:  # verified, so a client cannot dodge its limit by inventing a new "sub" per request
            sub = jwt.decode(auth[7:], config.JWT_SECRET, algorithms=["HS256"]).get("sub")
            if sub:
                return f"user:{sub}"
        except jwt.InvalidTokenError:
            pass
    return "ip:" + (request.client.host if request.client else "-")


def _allow(key: str) -> bool:
    now = time.monotonic()
    if len(buckets) > 10_000:  # bound memory: drop buckets idle for more than a minute
        for k in [k for k, (_, ts) in buckets.items() if now - ts > 60]:
            del buckets[k]
    tokens, last = buckets.get(key, [config.RATE_LIMIT_BURST, now])
    tokens = min(config.RATE_LIMIT_BURST, tokens + (now - last) * config.RATE_LIMIT_RPS)
    ok = tokens >= 1
    buckets[key] = [tokens - 1 if ok else tokens, now]
    return ok


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    full = "/" + path
    target = next((svc for prefix, svc in ROUTES if full == prefix or full.startswith(prefix + "/")), None)
    if target is None or target not in pools:
        return JSONResponse({"detail": "no route"}, status_code=404)
    request.state.route_label = f"proxy:{target}"
    if full.startswith("/internal") or "/internal/" in full:
        security_event("internal_via_gateway", request)
        return JSONResponse({"detail": "not found"}, status_code=404)

    if config.FT_RATE_LIMIT and not _allow(_principal_key(request)):
        RATE_LIMITED.inc()
        security_event("rate_limited", request, key=_principal_key(request))
        return JSONResponse({"detail": "rate limit exceeded"}, status_code=429, headers={"retry-after": "1"})

    body = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "content-length", "connection", "x-internal-token")}
    headers["x-request-id"] = request_id_var.get()
    headers["x-forwarded-for"] = request.client.host if request.client else "-"
    retry_safe = request.method in ("GET", "HEAD") or "idempotency-key" in {k.lower() for k in request.headers}

    pool = pools[target]
    cands = pool.candidates()
    if not (config.GW_FAILOVER and retry_safe):
        cands = cands[:1]
    last_err = None
    for i, url in enumerate(cands):
        try:
            r = await client.request(request.method, url + full, params=request.query_params,
                                     content=body, headers=headers)
            if r.status_code in (502, 503) and config.GW_FAILOVER and retry_safe and i < len(cands) - 1 \
                    and r.headers.get("retry-after") is None:
                pool.mark(url, False)
                FAILOVERS.labels(target).inc()
                log(logger, logging.WARNING, "failover", upstream=target, url=url, error=f"http_{r.status_code}")
                continue
            out_headers = {k: v for k, v in r.headers.items()
                           if k.lower() in ("content-type", "retry-after", "idempotent-replay", "x-served-by")}
            return Response(r.content, status_code=r.status_code, headers=out_headers)
        except httpx.HTTPError as exc:
            last_err = exc
            pool.mark(url, False)
            if i < len(cands) - 1:
                FAILOVERS.labels(target).inc()
                log(logger, logging.WARNING, "failover", upstream=target, url=url, error=type(exc).__name__)
    log(logger, logging.ERROR, "upstream_unavailable", upstream=target, error=type(last_err).__name__)
    code = 504 if isinstance(last_err, httpx.TimeoutException) else 502
    return JSONResponse({"detail": f"{target} unavailable"}, status_code=code)
