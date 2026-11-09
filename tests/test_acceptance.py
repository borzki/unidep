"""Acceptance tests against a running stack (improved profile).

    python deploy/local/supervisor.py start --mode improved &   # or docker compose up
    pytest -q tests/test_acceptance.py
Skipped automatically when the gateway is not reachable.
"""
import os
import uuid

import httpx
import pytest

GW = os.getenv("GATEWAY_URL", "http://127.0.0.1:8000")


def _up():
    try:
        return httpx.get(GW + "/health/ready", timeout=1).status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(not _up(), reason="stack not running")


def login(user, pw):
    r = httpx.post(GW + "/auth/login", json={"username": user, "password": pw})
    assert r.status_code == 200, r.text
    return {"authorization": "Bearer " + r.json()["access_token"]}


@pytest.fixture(scope="module")
def student():
    return login("student0010", "student-pass")


@pytest.fixture(scope="module")
def admin():
    return login("admin", "admin-pass")


def test_AC1_enrollment_workflow_confirms_and_notifies(student):
    key = uuid.uuid4().hex
    r = httpx.post(GW + "/enrollments", json={"course_id": 3}, headers=student | {"idempotency-key": key})
    assert r.status_code in (201, 409), r.text   # 409 if already enrolled by a previous test run
    if r.status_code == 201:
        assert r.json()["status"] == "CONFIRMED"


def test_AC2_idempotent_replay_returns_same_enrollment(student):
    key = uuid.uuid4().hex
    h = student | {"idempotency-key": key}
    r1 = httpx.post(GW + "/enrollments", json={"course_id": 4}, headers=h)
    r2 = httpx.post(GW + "/enrollments", json={"course_id": 4}, headers=h)
    if r1.status_code == 201:
        assert r2.status_code == 200 and r2.headers.get("idempotent-replay") == "true"
        assert r1.json()["id"] == r2.json()["id"]


def test_AC3_duplicate_enrollment_rejected(student):
    httpx.post(GW + "/enrollments", json={"course_id": 5}, headers=student | {"idempotency-key": uuid.uuid4().hex})
    r = httpx.post(GW + "/enrollments", json={"course_id": 5}, headers=student | {"idempotency-key": uuid.uuid4().hex})
    assert r.status_code == 409


def test_AC4_missing_idempotency_key_rejected(student):
    r = httpx.post(GW + "/enrollments", json={"course_id": 6}, headers=student)
    assert r.status_code == 400


def test_AC5_authentication_required():
    assert httpx.get(GW + "/courses").status_code == 401


def test_AC6_rbac_student_cannot_create_course(student, admin):
    assert httpx.post(GW + "/courses", json={"code": "X-1", "title": "t", "capacity": 5},
                      headers=student).status_code == 403


def test_AC7_input_validation(admin):
    r = httpx.post(GW + "/courses", json={"code": "CS'; DROP TABLE x;--", "title": "t", "capacity": 5}, headers=admin)
    assert r.status_code == 422


def test_AC8_internal_endpoints_not_exposed(student):
    assert httpx.post(GW + "/courses/internal/reservations", json={}, headers=student).status_code == 404


def test_AC9_readiness_reports_dependencies():
    r = httpx.get("http://127.0.0.1:8301/health/ready")
    assert r.status_code == 200 and r.json()["checks"]["database"] is True
