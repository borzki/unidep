"""Shared FastAPI app factory: health/readiness, body-size limit, observability, chaos."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import chaos, config, obs
from .db import Database
from .security import security_event


def create_app(service: str, db: Database | None, on_start=None, on_stop=None, extra_ready=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if db is not None:
            await db.connect()
        if on_start:
            await on_start()
        yield
        if on_stop:
            await on_stop()
        if db is not None:
            await db.close()

    app = FastAPI(title=f"unidep-{service}", lifespan=lifespan)

    # Order matters: middleware added last runs first. Chaos is innermost so that
    # injected failures are still recorded by the observability middleware.
    chaos.install(app)

    @app.middleware("http")
    async def body_limit(request: Request, call_next):
        cl = request.headers.get("content-length")
        if request.method in ("POST", "PUT", "PATCH") and cl is None and request.headers.get("transfer-encoding"):
            security_event("chunked_body_rejected", request)
            return JSONResponse({"detail": "Content-Length required"}, status_code=411)
        if cl and cl.isdigit() and int(cl) > config.MAX_BODY_BYTES:
            security_event("oversized_payload", request, size=int(cl))
            return JSONResponse({"detail": "payload too large"}, status_code=413)
        return await call_next(request)

    obs.install(app, service)

    @app.get("/health/live")
    async def live():
        return {"status": "alive", "service": service, "instance": config.INSTANCE_ID}

    @app.get("/health/ready")
    async def ready():
        checks = {}
        if db is not None:
            checks["database"] = await db.ping()
        if extra_ready:
            checks.update(extra_ready())
        ok = all(v for k, v in checks.items() if not k.startswith("info_"))
        return JSONResponse({"ready": ok, "checks": checks, "config": config.describe()},
                            status_code=200 if ok else 503)

    return app
