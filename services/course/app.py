"""course-service: course catalogue and seat reservations (idempotent per enrollment_id)."""
import uuid

import asyncpg
from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field

from common import config
from common.app_base import create_app
from common.db import Database
from common.security import Principal, current_user, internal_only, require_role

db = Database("course")
app = create_app("course", db)


class CourseIn(BaseModel):
    code: str = Field(min_length=2, max_length=16, pattern=r"^[A-Z0-9-]+$")
    title: str = Field(min_length=1, max_length=200)
    capacity: int = Field(gt=0, le=1000)


class ReservationIn(BaseModel):
    enrollment_id: uuid.UUID
    course_id: int = Field(gt=0)
    student_id: int = Field(gt=0)


@app.get("/courses")
async def list_courses(user: Principal = Depends(current_user)):
    async def q(conn):
        return await conn.fetch("SELECT id, code, title, capacity, seats_taken FROM courses ORDER BY id")
    return [dict(r) for r in await db.run(q)]


@app.get("/courses/{course_id}")
async def get_course(course_id: int, user: Principal = Depends(current_user)):
    async def q(conn):
        return await conn.fetchrow("SELECT id, code, title, capacity, seats_taken FROM courses WHERE id=$1", course_id)
    row = await db.run(q)
    if not row:
        raise HTTPException(404, "course not found")
    return dict(row)


@app.post("/courses", status_code=201)
async def create_course(body: CourseIn, user: Principal = Depends(require_role("admin"))):
    async def q(conn):
        return await conn.fetchrow(
            "INSERT INTO courses(code, title, capacity) VALUES($1,$2,$3) RETURNING id, code, title, capacity, seats_taken",
            body.code, body.title, body.capacity)
    try:
        return dict(await db.run(q))
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "course code exists")


@app.post("/internal/reservations", dependencies=[Depends(internal_only)])
async def reserve(body: ReservationIn):
    """Reserve one seat for an enrollment. With FT_IDEMPOTENCY the operation is
    idempotent on enrollment_id: a repeated call returns the existing reservation."""
    async def q(conn):
        if config.FT_IDEMPOTENCY:
            existing = await conn.fetchrow("SELECT id FROM reservations WHERE enrollment_id=$1", body.enrollment_id)
            if existing:
                return {"reservation_id": existing["id"], "replayed": True}
        updated = await conn.fetchrow(
            "UPDATE courses SET seats_taken = seats_taken + 1 WHERE id=$1 AND seats_taken < capacity RETURNING id",
            body.course_id)
        if not updated:
            exists = await conn.fetchval("SELECT 1 FROM courses WHERE id=$1", body.course_id)
            raise HTTPException(404 if not exists else 409, "course not found" if not exists else "course full")
        rid = await conn.fetchval(
            "INSERT INTO reservations(enrollment_id, course_id, student_id) VALUES($1,$2,$3) RETURNING id",
            body.enrollment_id, body.course_id, body.student_id)
        return {"reservation_id": rid, "replayed": False}
    try:
        return await db.run(q)
    except asyncpg.UniqueViolationError:
        # concurrent duplicate on the unique index -> treat as replay
        async def q2(conn):
            return await conn.fetchval("SELECT id FROM reservations WHERE enrollment_id=$1", body.enrollment_id)
        return {"reservation_id": await db.run(q2), "replayed": True}


@app.get("/internal/reservations/{enrollment_id}", dependencies=[Depends(internal_only)])
async def get_reservation(enrollment_id: uuid.UUID):
    async def q(conn):
        return await conn.fetchrow("SELECT id, course_id FROM reservations WHERE enrollment_id=$1", enrollment_id)
    row = await db.run(q)
    if not row:
        raise HTTPException(404, "no reservation")
    return dict(row)


@app.delete("/internal/reservations/{enrollment_id}", dependencies=[Depends(internal_only)])
async def release(enrollment_id: uuid.UUID):
    """Compensation: release the seat held by an enrollment (idempotent)."""
    async def q(conn):
        rows = await conn.fetch("DELETE FROM reservations WHERE enrollment_id=$1 RETURNING course_id", enrollment_id)
        for r in rows:
            await conn.execute("UPDATE courses SET seats_taken = seats_taken - 1 WHERE id=$1", r["course_id"])
        return len(rows)
    return {"released": await db.run(q)}
