-- UniDep schema. One PostgreSQL instance, one schema per service
-- (logical ownership boundary: a service only touches its own schema).

CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS course;
CREATE SCHEMA IF NOT EXISTS enrollment;
CREATE SCHEMA IF NOT EXISTS notification;

CREATE TABLE IF NOT EXISTS auth.users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(64) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role          VARCHAR(16) NOT NULL CHECK (role IN ('student', 'admin'))
);

CREATE TABLE IF NOT EXISTS auth.audit_log (
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    username   VARCHAR(64),
    event      VARCHAR(64) NOT NULL,
    client     VARCHAR(64),
    detail     TEXT
);

CREATE TABLE IF NOT EXISTS course.courses (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(16) UNIQUE NOT NULL,
    title       VARCHAR(200) NOT NULL,
    capacity    INT NOT NULL CHECK (capacity > 0),
    seats_taken INT NOT NULL DEFAULT 0,
    CONSTRAINT seats_within_capacity CHECK (seats_taken >= 0 AND seats_taken <= capacity)
);

-- One row per reserved seat. enrollment_id links back to enrollment.enrollments.
-- (No UNIQUE on enrollment_id here: in the improved profile duplicates are
-- prevented by the application + index created at startup, see
-- services/course/app.py, so that the baseline can exhibit the failure.)
CREATE TABLE IF NOT EXISTS course.reservations (
    id            BIGSERIAL PRIMARY KEY,
    enrollment_id UUID NOT NULL,
    course_id     INT NOT NULL REFERENCES course.courses(id),
    student_id    INT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS enrollment.enrollments (
    id              UUID PRIMARY KEY,
    idempotency_key VARCHAR(80),
    student_id      INT NOT NULL,
    course_id       INT NOT NULL,
    status          VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'FAILED', 'CANCELLED')),
    step            VARCHAR(32) NOT NULL,        -- checkpoint of the multi-step workflow
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS enrollments_status_idx ON enrollment.enrollments(status, updated_at);

-- Transactional outbox for notifications (graceful degradation).
CREATE TABLE IF NOT EXISTS enrollment.outbox (
    id            BIGSERIAL PRIMARY KEY,
    enrollment_id UUID NOT NULL,
    student_id    INT NOT NULL,
    message       TEXT NOT NULL,
    attempts      INT NOT NULL DEFAULT 0,
    sent_at       TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notification.notifications (
    id            BIGSERIAL PRIMARY KEY,
    enrollment_id UUID NOT NULL,
    student_id    INT NOT NULL,
    message       TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
