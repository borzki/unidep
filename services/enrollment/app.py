"""enrollment-service: orchestrates the critical multi-step workflow

    1. create enrollment record (PENDING, step=CREATED)          [checkpoint]
    2. reserve seat in course-service (idempotent on enrollment id)
    3. mark CONFIRMED (step=RESERVED) + enqueue notification       [checkpoint]
    4. deliver notification (outbox dispatcher, or synchronous in baseline)

Mechanisms: Idempotency-Key + duplicate detection, timeouts, bounded retries,
circuit breaker, checkpoint/recovery of interrupted workflows, compensation
(seat release on cancel), graceful degradation via transactional outbox.
"""
import asyncio
import logging
import uuid

import asyncpg
import httpx
from fastapi import Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from common import chaos, config
from common.app_base import create_app
from common.db import Database
from common.obs import ENROLLMENTS, RECOVERY_ACTIONS, get_logger, log
from common.resilience import CircuitOpenError, ResilientClient, UpstreamError
from common.security import Principal, current_user, require_role, security_event

logger = get_logger("enrollment")
db = Database("enrollment")
course_client: ResilientClient | None = None
notif_client: ResilientClient | None = None
_tasks: list[asyncio.Task] = []

DEP_ERRORS = (httpx.TransportError, UpstreamError, CircuitOpenError)


async def _start():
    global course_client, notif_client
    course_client = ResilientClient("course", config.COURSE_URLS)
    notif_client = ResilientClient("notification", config.NOTIFICATION_URLS)
    if config.FT_RECOVERY:
        _tasks.append(asyncio.create_task(_recovery_loop()))
    if config.FT_OUTBOX:
        _tasks.append(asyncio.create_task(_outbox_loop()))
    _tasks.append(asyncio.create_task(_breaker_tick()))


async def _breaker_tick():
    """Keep exported breaker state fresh even when no calls are made."""
    while True:
        await asyncio.sleep(1.0)
        for c in (course_client, notif_client):
            if c:
                c.breaker.refresh()


async def _stop():
    for t in _tasks:
        t.cancel()
    for c in (course_client, notif_client):
        if c:
            await c.aclose()


def _ready_extra():
    return {"info_course_breaker": course_client.breaker.state if course_client else "n/a",
            "info_notification_breaker": notif_client.breaker.state if notif_client else "n/a"}


app = create_app("enrollment", db, on_start=_start, on_stop=_stop, extra_ready=_ready_extra)


class EnrollIn(BaseModel):
    course_id: int = Field(gt=0, le=10_000_000)


def _row(r) -> dict:
    return {"id": str(r["id"]), "student_id": r["student_id"], "course_id": r["course_id"],
            "status": r["status"], "step": r["step"], "reason": r["reason"]}


async def _set_status(eid, status, step, reason=None, outbox_msg=None, student_id=None):
    async def q(conn):
        r = await conn.fetchrow(
            "UPDATE enrollments SET status=$2, step=$3, reason=$4, updated_at=now() WHERE id=$1 RETURNING *",
            eid, status, step, reason)
        if outbox_msg and config.FT_OUTBOX:
            await conn.execute("INSERT INTO outbox(enrollment_id, student_id, message) VALUES($1,$2,$3)",
                               eid, student_id, outbox_msg)
        return r
    return await db.run(q)


def _terminal_code(row) -> int:
    """HTTP status for an enrollment that is already in a terminal state, so a
    replay returns the same outcome as the original request."""
    if row["status"] == "FAILED":
        return 404 if row["reason"] == "not_found" else 409
    return 200


async def _confirm(eid, sid, msg):
    """Checkpoint 2: PENDING -> CONFIRMED (+ outbox row) in one transaction.
    Guarded on status so a late writer never overwrites CANCELLED/CONFIRMED."""
    async def q(conn):
        r = await conn.fetchrow(
            "UPDATE enrollments SET status='CONFIRMED', step='RESERVED', reason=NULL, updated_at=now() "
            "WHERE id=$1 AND status='PENDING' RETURNING *", eid)
        if r is not None and config.FT_OUTBOX:
            await conn.execute("INSERT INTO outbox(enrollment_id, student_id, message) VALUES($1,$2,$3)",
                               eid, sid, msg)
        return r
    return await db.run(q)


async def _reserve(eid, course_id, student_id) -> str:
    """Returns 'reserved' | 'full' | 'not_found'. Raises DEP_ERRORS when unknown."""
    r = await course_client.request("POST", "/internal/reservations", idempotent=config.FT_IDEMPOTENCY,
                                    json={"enrollment_id": str(eid), "course_id": course_id, "student_id": student_id})
    if r.status_code == 409:
        return "full"
    if r.status_code == 404:
        return "not_found"
    r.raise_for_status()
    return "reserved"


async def _notify_sync(eid, student_id, msg):
    await notif_client.request("POST", "/internal/notifications", idempotent=config.FT_IDEMPOTENCY,
                               json={"enrollment_id": str(eid), "student_id": student_id, "message": msg})


async def _continue_workflow(row) -> tuple[int, dict]:
    """Drive an enrollment from its last checkpoint to a terminal state."""
    eid, sid, cid = row["id"], row["student_id"], row["course_id"]
    if row["status"] != "PENDING":
        return _terminal_code(row), _row(row)
    try:
        outcome = await _reserve(eid, cid, sid)
    except DEP_ERRORS as exc:
        ENROLLMENTS.labels("dependency_unavailable").inc()
        log(logger, logging.WARNING, "reserve_failed", enrollment_id=str(eid), error=type(exc).__name__)
        if config.FT_RECOVERY:
            # outcome unknown: keep PENDING, recovery loop / client retry will finish it
            return 503, {"detail": "course service unavailable; enrollment accepted as PENDING, "
                                   "retry with the same Idempotency-Key", "id": str(eid), "status": "PENDING"}
        await _set_status(eid, "FAILED", "RESERVE_ERROR", reason=type(exc).__name__)
        return 503, {"detail": "course service unavailable", "id": str(eid), "status": "FAILED"}
    if outcome != "reserved":
        r = await _set_status(eid, "FAILED", "REJECTED", reason=outcome)
        ENROLLMENTS.labels(outcome).inc()
        return (409 if outcome == "full" else 404), _row(r)

    chaos.crash_if("after_reserve")  # fault injection: crash between step 2 and 3

    msg = f"You are enrolled in course {cid}."
    r = await _confirm(eid, sid, msg)
    if r is None:
        # someone else finished this enrollment first (replica, recovery loop) or it was cancelled
        async def cur(conn):
            return await conn.fetchrow("SELECT * FROM enrollments WHERE id=$1", eid)
        current = await db.run(cur)
        if current["status"] in ("CANCELLED", "FAILED"):
            # we may have (re-)reserved a seat after the cancel released it: compensate
            try:
                await course_client.request("DELETE", f"/internal/reservations/{eid}", idempotent=True)
                RECOVERY_ACTIONS.labels("enrollment", "late_compensation").inc()
            except DEP_ERRORS:
                log(logger, logging.ERROR, "late_compensation_failed", enrollment_id=str(eid))
        return _terminal_code(current), _row(current)
    ENROLLMENTS.labels("confirmed").inc()
    if not config.FT_OUTBOX:
        # baseline: notification is on the critical path
        try:
            await _notify_sync(eid, sid, msg)
        except DEP_ERRORS as exc:
            log(logger, logging.ERROR, "notification_failed", enrollment_id=str(eid), error=type(exc).__name__)
            return 500, {"detail": "notification failed", "id": str(eid)}
    return 201, _row(r)


@app.post("/enrollments")
async def enroll(body: EnrollIn, request: Request, user: Principal = Depends(current_user),
                 idempotency_key: str | None = Header(default=None, max_length=80)):
    if user.role != "student":
        raise HTTPException(403, "only students enroll")
    if config.FT_IDEMPOTENCY:
        if not idempotency_key:
            raise HTTPException(400, "Idempotency-Key header required")
        key = f"{user.user_id}:{idempotency_key}"   # scope keys per user
    else:
        key = None

    async def create(conn):
        if key:
            existing = await conn.fetchrow("SELECT * FROM enrollments WHERE idempotency_key=$1", key)
            if existing:
                if existing["course_id"] != body.course_id:
                    raise HTTPException(422, "Idempotency-Key reused with a different request")
                return existing, True
        r = await conn.fetchrow(
            "INSERT INTO enrollments(id, idempotency_key, student_id, course_id, status, step) "
            "VALUES($1,$2,$3,$4,'PENDING','CREATED') RETURNING *",
            uuid.uuid4(), key, user.user_id, body.course_id)
        return r, False

    try:
        row, replay = await db.run(create)
    except asyncpg.UniqueViolationError as exc:
        if "idem" in (exc.constraint_name or ""):
            async def again(conn):
                return await conn.fetchrow("SELECT * FROM enrollments WHERE idempotency_key=$1", key)
            row, replay = await db.run(again), True
        else:
            ENROLLMENTS.labels("duplicate_rejected").inc()
            raise HTTPException(409, "already enrolled or enrollment in progress for this course")
    if replay:
        ENROLLMENTS.labels("idempotent_replay").inc()
        log(logger, logging.INFO, "idempotent_replay", enrollment_id=str(row["id"]), status=row["status"])
    status, payload = await _continue_workflow(row)
    headers = {"idempotent-replay": "true"} if replay else {}
    if status == 503:
        headers["retry-after"] = "2"
    return JSONResponse(payload, status_code=200 if (replay and status == 201) else status, headers=headers)


@app.get("/enrollments/{eid}")
async def get_enrollment(eid: uuid.UUID, request: Request, user: Principal = Depends(current_user)):
    async def q(conn):
        return await conn.fetchrow("SELECT * FROM enrollments WHERE id=$1", eid)
    r = await db.run(q)
    if not r:
        raise HTTPException(404, "not found")
    if user.role != "admin" and r["student_id"] != user.user_id:   # object-level authorisation (IDOR)
        security_event("idor_denied", request, user=user.username, enrollment_id=str(eid))
        raise HTTPException(404, "not found")
    return _row(r)


@app.get("/enrollments")
async def my_enrollments(user: Principal = Depends(current_user)):
    async def q(conn):
        return await conn.fetch("SELECT * FROM enrollments WHERE student_id=$1 ORDER BY created_at DESC LIMIT 100",
                                user.user_id)
    return [_row(r) for r in await db.run(q)]


@app.post("/enrollments/{eid}/cancel")
async def cancel(eid: uuid.UUID, request: Request, user: Principal = Depends(current_user)):
    """Rollback/compensation: release seat, mark CANCELLED."""
    async def q(conn):
        return await conn.fetchrow("SELECT * FROM enrollments WHERE id=$1", eid)
    r = await db.run(q)
    if not r or (user.role != "admin" and r["student_id"] != user.user_id):
        if r:
            security_event("idor_denied", request, user=user.username, enrollment_id=str(eid))
        raise HTTPException(404, "not found")
    if r["status"] in ("CANCELLED", "FAILED"):
        return _row(r)
    try:
        await course_client.request("DELETE", f"/internal/reservations/{eid}", idempotent=True)
    except DEP_ERRORS:
        raise HTTPException(503, "course service unavailable, cancel not applied")
    return _row(await _set_status(eid, "CANCELLED", "RELEASED"))


@app.post("/admin/recovery/run")
async def run_recovery(user: Principal = Depends(require_role("admin"))):
    return {"recovered": await recover_once(min_age=0)}


# ------------------------------------------------------------ background work
async def recover_once(min_age: float | None = None) -> int:
    """Checkpoint recovery: finish PENDING enrollments abandoned by a crashed
    request (roll forward; reservation is idempotent so this never double-books)."""
    min_age = config.RECOVERY_MIN_AGE_S if min_age is None else min_age

    async def claim(conn):
        # SKIP LOCKED lets several replicas run recovery without stepping on each other
        return await conn.fetch(
            "SELECT * FROM enrollments WHERE status='PENDING' AND updated_at < now() - make_interval(secs => $1) "
            "ORDER BY updated_at LIMIT 20 FOR UPDATE SKIP LOCKED", min_age)
    rows = await db.run(claim)
    done = 0
    for row in rows:
        async def touch(conn, eid=row["id"]):
            await conn.execute("UPDATE enrollments SET updated_at=now() WHERE id=$1 AND status='PENDING'", eid)
        await db.run(touch)
        status, _ = await _continue_workflow(row)
        if status != 503:
            done += 1
            RECOVERY_ACTIONS.labels("enrollment", "pending_completed").inc()
            log(logger, logging.WARNING, "recovered_enrollment", enrollment_id=str(row["id"]), result=status)
    return done


async def _recovery_loop():
    while True:
        await asyncio.sleep(config.RECOVERY_INTERVAL_S)
        try:
            await recover_once()
        except Exception as exc:  # noqa: BLE001
            log(logger, logging.WARNING, "recovery_loop_error", error=type(exc).__name__)


async def dispatch_outbox_once() -> int:
    async def claim(conn):
        return await conn.fetch("SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED")

    sent = 0
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            rows = await claim(conn)
            for row in rows:
                try:
                    await _notify_sync(row["enrollment_id"], row["student_id"], row["message"])
                except DEP_ERRORS:
                    await conn.execute("UPDATE outbox SET attempts=attempts+1 WHERE id=$1", row["id"])
                    break  # dependency down: stop this batch, try again next tick
                await conn.execute("UPDATE outbox SET sent_at=now(), attempts=attempts+1 WHERE id=$1", row["id"])
                sent += 1
    if sent:
        RECOVERY_ACTIONS.labels("enrollment", "outbox_sent").inc(sent)
    return sent


async def _outbox_loop():
    while True:
        await asyncio.sleep(config.OUTBOX_INTERVAL_S)
        try:
            await dispatch_outbox_once()
        except Exception as exc:  # noqa: BLE001
            log(logger, logging.WARNING, "outbox_loop_error", error=type(exc).__name__)
