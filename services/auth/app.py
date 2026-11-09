"""auth-service: login (JWT issuance), brute-force lockout, audit log."""
import logging
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request
from pydantic import BaseModel, Field

from common import config
from common.app_base import create_app
from common.db import Database
from common.obs import get_logger, log
from common.security import issue_token, security_event, verify_password

db = Database("auth")
app = create_app("auth", db)
logger = get_logger("auth")

_failures: dict[str, deque] = defaultdict(deque)  # username -> timestamps of failed logins


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(min_length=1, max_length=128)


async def _audit(username: str, event: str, client: str, detail: str = ""):
    async def q(conn):
        await conn.execute("INSERT INTO audit_log(username, event, client, detail) VALUES($1,$2,$3,$4)",
                           username, event, client, detail)
    try:
        await db.run(q)
    except Exception:  # audit must not take login down; the JSON log still has it
        log(logger, logging.ERROR, "audit_write_failed", username=username, event=event)


@app.post("/auth/login")
async def login(body: LoginIn, request: Request):
    client = request.headers.get("x-forwarded-for") or (request.client.host if request.client else "-")
    now = time.time()
    window = _failures[body.username]
    while window and now - window[0] > config.LOGIN_WINDOW_S:
        window.popleft()
    if len(window) >= config.LOGIN_MAX_FAILURES:
        security_event("login_locked", request, user=body.username)
        await _audit(body.username, "login_locked", client)
        raise HTTPException(429, "too many failed attempts, try later")

    async def q(conn):
        return await conn.fetchrow("SELECT id, password_hash, role FROM users WHERE username=$1", body.username)
    row = await db.run(q)
    if row is None or not verify_password(body.password, row["password_hash"]):
        window.append(now)
        security_event("login_failed", request, user=body.username)
        await _audit(body.username, "login_failed", client)
        raise HTTPException(401, "invalid credentials")
    window.clear()
    await _audit(body.username, "login_ok", client)
    return {"access_token": issue_token(row["id"], body.username, row["role"]), "token_type": "bearer",
            "role": row["role"]}
