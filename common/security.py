"""Authentication (JWT HS256), role-based access control, internal-call auth,
audit logging of security events."""
import hashlib
import hmac
import logging
import os
import time

import jwt
from fastapi import Depends, Header, HTTPException, Request

from . import config
from .obs import SECURITY_EVENTS, get_logger, log

audit = get_logger("audit")


def security_event(event: str, request: Request | None = None, **fields):
    SECURITY_EVENTS.labels(config.SERVICE_NAME, event).inc()
    if request is not None:
        fields.setdefault("path", request.url.path)
        fields.setdefault("client", request.headers.get("x-forwarded-for") or (request.client.host if request.client else "-"))
    log(audit, logging.WARNING, "security_event", event=event, **fields)


def hash_password(password: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return salt.hex() + "$" + dk.hex()


def verify_password(password: str, stored: str) -> bool:
    salt_hex, dk_hex = stored.split("$", 1)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), 100_000)
    return hmac.compare_digest(dk.hex(), dk_hex)


def issue_token(user_id: int, username: str, role: str, ttl: int | None = None) -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": str(user_id), "username": username, "role": role, "iat": now,
         "exp": now + (ttl if ttl is not None else config.JWT_TTL_S)},
        config.JWT_SECRET, algorithm="HS256",
    )


class Principal:
    def __init__(self, user_id: int, username: str, role: str):
        self.user_id, self.username, self.role = user_id, username, role


def current_user(request: Request, authorization: str | None = Header(default=None)) -> Principal:
    if not authorization or not authorization.lower().startswith("bearer "):
        security_event("missing_token", request)
        raise HTTPException(401, "authentication required")
    token = authorization.split(" ", 1)[1]
    try:
        claims = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"], options={"require": ["exp", "sub"]})
    except jwt.ExpiredSignatureError:
        security_event("expired_token", request)
        raise HTTPException(401, "token expired")
    except jwt.InvalidTokenError:
        security_event("invalid_token", request)
        raise HTTPException(401, "invalid token")
    return Principal(int(claims["sub"]), claims.get("username", ""), claims.get("role", "student"))


def require_role(*roles: str):
    def dep(request: Request, user: Principal = Depends(current_user)) -> Principal:
        if user.role not in roles:
            security_event("forbidden_role", request, user=user.username, role=user.role, needed=list(roles))
            raise HTTPException(403, "insufficient role")
        return user
    return dep


def internal_only(request: Request, x_internal_token: str | None = Header(default=None)):
    if not x_internal_token or not hmac.compare_digest(x_internal_token, config.INTERNAL_TOKEN):
        security_event("internal_endpoint_denied", request)
        raise HTTPException(403, "internal endpoint")
