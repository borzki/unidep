"""notification-service: stores user notifications (dedupe on enrollment_id)."""
import uuid

import asyncpg
from fastapi import Depends
from pydantic import BaseModel, Field

from common import config
from common.app_base import create_app
from common.db import Database
from common.security import Principal, current_user, internal_only

db = Database("notification")
app = create_app("notification", db)


class NotificationIn(BaseModel):
    enrollment_id: uuid.UUID
    student_id: int = Field(gt=0)
    message: str = Field(min_length=1, max_length=500)


@app.post("/internal/notifications", dependencies=[Depends(internal_only)])
async def create(body: NotificationIn):
    async def q(conn):
        if config.FT_IDEMPOTENCY:
            if await conn.fetchval("SELECT 1 FROM notifications WHERE enrollment_id=$1", body.enrollment_id):
                return {"stored": False, "duplicate": True}
        await conn.execute("INSERT INTO notifications(enrollment_id, student_id, message) VALUES($1,$2,$3)",
                           body.enrollment_id, body.student_id, body.message)
        return {"stored": True, "duplicate": False}
    try:
        return await db.run(q)
    except asyncpg.UniqueViolationError:
        return {"stored": False, "duplicate": True}


@app.get("/notifications")
async def mine(user: Principal = Depends(current_user)):
    async def q(conn):
        return await conn.fetch("SELECT enrollment_id, message, created_at FROM notifications WHERE student_id=$1 "
                                "ORDER BY id DESC LIMIT 50", user.user_id)
    return [dict(r) for r in await db.run(q)]
