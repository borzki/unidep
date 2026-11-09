"""Reset + seed the database for a given resilience profile.

    python -m db.reset --mode improved --students 200 --courses 10 --capacity 100

Integrity constraints that are part of the *improved* design (unique
idempotency key, one active enrollment per student/course, one reservation
per enrollment, one notification per enrollment) are created only in the
improved profile so that the baseline can exhibit duplicate records.
Passwords are synthetic test values; no personal data is used.
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.security import hash_password  # noqa: E402

IMPROVED_CONSTRAINTS = [
    "CREATE UNIQUE INDEX enrollments_idem_key ON enrollment.enrollments(idempotency_key)",
    "CREATE UNIQUE INDEX enrollments_active_uniq ON enrollment.enrollments(student_id, course_id) "
    "WHERE status IN ('PENDING','CONFIRMED')",
    "CREATE UNIQUE INDEX reservations_enrollment_uniq ON course.reservations(enrollment_id)",
    "CREATE UNIQUE INDEX notifications_enrollment_uniq ON notification.notifications(enrollment_id)",
]

STUDENT_PASSWORD = "student-pass"   # test-only
ADMIN_PASSWORD = "admin-pass"       # test-only


async def reset(dsn: str, mode: str, students: int, courses: int, capacity: int, if_empty: bool = False):
    admin_dsn = dsn.rsplit("/", 1)[0] + "/postgres"
    dbname = dsn.rsplit("/", 1)[1]
    c = await asyncpg.connect(admin_dsn)
    if not await c.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", dbname):
        await c.execute(f'CREATE DATABASE "{dbname}"')
    await c.close()

    conn = await asyncpg.connect(dsn)
    if if_empty and await conn.fetchval("SELECT to_regclass('auth.users') IS NOT NULL"):
        await conn.close()
        print("database already initialised; keeping existing data (--if-empty)")
        return
    await conn.execute("DROP SCHEMA IF EXISTS auth, course, enrollment, notification CASCADE")
    await conn.execute((Path(__file__).parent / "init.sql").read_text())
    if mode == "improved":
        for stmt in IMPROVED_CONSTRAINTS:
            await conn.execute(stmt)
    # one hash reused for all synthetic students keeps seeding fast
    sh = hash_password(STUDENT_PASSWORD)
    await conn.execute("INSERT INTO auth.users(username, password_hash, role) VALUES('admin', $1, 'admin')",
                       hash_password(ADMIN_PASSWORD))
    await conn.executemany("INSERT INTO auth.users(username, password_hash, role) VALUES($1, $2, 'student')",
                           [(f"student{i:04d}", sh) for i in range(1, students + 1)])
    await conn.executemany("INSERT INTO course.courses(code, title, capacity) VALUES($1,$2,$3)",
                           [(f"CS-{100 + i}", f"Course {i}", capacity) for i in range(1, courses + 1)])
    await conn.close()
    print(f"reset ok mode={mode} students={students} courses={courses} capacity={capacity}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.getenv("DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/unidep"))
    ap.add_argument("--mode", choices=["baseline", "improved"], default="improved")
    ap.add_argument("--students", type=int, default=200)
    ap.add_argument("--courses", type=int, default=10)
    ap.add_argument("--capacity", type=int, default=1000)
    ap.add_argument("--if-empty", action="store_true", help="only initialise when the schema does not exist yet")
    a = ap.parse_args()
    asyncio.run(reset(a.dsn, a.mode, a.students, a.courses, a.capacity, a.if_empty))
