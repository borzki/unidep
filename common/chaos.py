"""Controlled fault injection (authorised test environments only).

POST /chaos  {"latency_ms": 800, "error_rate": 0.0, "crash_point": "after_reserve", "path_prefix": "/internal"}
Protected by X-Chaos-Token. Disabled entirely unless CHAOS_ENABLED=1.
"""
import asyncio
import hmac
import os
import random

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from . import config
from .obs import get_logger, log
import logging

logger = get_logger("chaos")
ENABLED = os.getenv("CHAOS_ENABLED", "0") == "1"


class ChaosState(BaseModel):
    latency_ms: int = Field(0, ge=0, le=60000)
    error_rate: float = Field(0.0, ge=0.0, le=1.0)
    crash_point: str | None = None
    path_prefix: str = "/"


state = ChaosState()


def crash_if(point: str):
    """Hard-crash the process at a named point (simulates power loss / OOM kill)."""
    if ENABLED and state.crash_point == point:
        log(logger, logging.CRITICAL, "chaos_crash", point=point)
        os._exit(137)


def install(app: FastAPI):
    if not ENABLED:
        return

    @app.post("/chaos", include_in_schema=False)
    async def set_chaos(new: ChaosState, x_chaos_token: str | None = Header(default=None)):
        if not x_chaos_token or not hmac.compare_digest(x_chaos_token, config.CHAOS_TOKEN):
            raise HTTPException(403, "chaos token required")
        global state
        if new.crash_point == "now":
            # hard crash shortly after replying (simulates a process dying)
            log(logger, logging.CRITICAL, "chaos_crash", point="now")
            asyncio.get_running_loop().call_later(0.3, lambda: os._exit(137))
            return new
        state = new
        log(logger, logging.WARNING, "chaos_set", **new.model_dump())
        return state

    @app.middleware("http")
    async def _inject(request: Request, call_next):
        p = request.url.path
        if p.startswith(state.path_prefix) and not p.startswith(("/chaos", "/metrics", "/health")):
            if state.latency_ms:
                await asyncio.sleep(state.latency_ms / 1000)
            if state.error_rate and random.random() < state.error_rate:
                return JSONResponse({"detail": "chaos injected error"}, status_code=503)
        return await call_next(request)
