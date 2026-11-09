"""E7 - unauthorized / invalid operations (authorised local environment only).

    python experiments/security_tests.py --modes baseline,improved
Writes results/raw/E7/<mode>/cases.json and audit evidence.
"""
import argparse
import asyncio
import json
import time
import uuid

import asyncpg
import httpx
import jwt

from lib import DSN, GW, ROOT, Env, prom_alerts, prom_counters, save_json

RAW = ROOT / "results" / "raw" / "E7"


def login(u, p):
    return httpx.post(GW + "/auth/login", json={"username": u, "password": p})


def bearer(tok):
    return {"authorization": "Bearer " + tok}


def run_cases(mode: str) -> list[dict]:
    from common import config
    cases = []

    def case(cid, name, expected, fn, note=""):
        try:
            got = fn()
        except Exception as exc:  # noqa: BLE001
            got = f"error:{type(exc).__name__}"
        ok = got in expected if isinstance(expected, (list, tuple)) else got == expected
        cases.append({"id": cid, "case": name, "expected": expected, "observed": got, "pass": ok, "note": note})
        print(f"   {cid} {'PASS' if ok else 'FAIL'} {name}: expected {expected} got {got}")

    s1 = login("student0001", "student-pass").json()["access_token"]
    s2 = login("student0002", "student-pass").json()["access_token"]
    adm = login("admin", "admin-pass").json()["access_token"]
    # an enrollment that belongs to student0001
    e1 = httpx.post(GW + "/enrollments", json={"course_id": 1},
                    headers=bearer(s1) | {"idempotency-key": uuid.uuid4().hex}).json().get("id")

    case("S1", "No token on protected endpoint", 401, lambda: httpx.get(GW + "/courses").status_code)
    forged = jwt.encode({"sub": "1", "role": "admin", "exp": int(time.time()) + 600},
                        "attacker-guessed-secret-0123456789abcdef", algorithm="HS256")
    case("S2", "Forged admin token (wrong signing key)", 401,
         lambda: httpx.post(GW + "/courses", json={"code": "HACK-1", "title": "x", "capacity": 1},
                            headers=bearer(forged)).status_code)
    import base64
    def b64(d):
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    none_tok = b64({"alg": "none", "typ": "JWT"}) + "." + b64({"sub": "1", "role": "admin",
                                                               "exp": int(time.time()) + 600}) + "."
    case("S3", "alg=none token", 401, lambda: httpx.get(GW + "/courses", headers=bearer(none_tok)).status_code)
    expired = jwt.encode({"sub": "2", "role": "student", "exp": int(time.time()) - 5}, config.JWT_SECRET,
                         algorithm="HS256")
    case("S4", "Expired token", 401, lambda: httpx.get(GW + "/courses", headers=bearer(expired)).status_code)
    case("S5", "Student calls admin-only operation (create course)", 403,
         lambda: httpx.post(GW + "/courses", json={"code": "X-2", "title": "x", "capacity": 3},
                            headers=bearer(s1)).status_code)
    case("S6", "IDOR: student reads another student's enrollment", 404,
         lambda: httpx.get(f"{GW}/enrollments/{e1}", headers=bearer(s2)).status_code)
    case("S7", "IDOR: student cancels another student's enrollment", 404,
         lambda: httpx.post(f"{GW}/enrollments/{e1}/cancel", headers=bearer(s2)).status_code)
    case("S8", "Internal API through gateway", 404,
         lambda: httpx.post(GW + "/courses/internal/reservations", json={}, headers=bearer(s1)).status_code)
    case("S9", "Internal API directly without service token", 403,
         lambda: httpx.post("http://127.0.0.1:8201/internal/reservations",
                            json={"enrollment_id": str(uuid.uuid4()), "course_id": 1, "student_id": 2}).status_code)
    case("S10", "SQL-injection payload in course code (admin)", 422,
         lambda: httpx.post(GW + "/courses", json={"code": "CS'; DROP TABLE course.courses;--", "title": "x",
                                                   "capacity": 3}, headers=bearer(adm)).status_code)
    case("S11", "SQL-injection payload as username", 422,
         lambda: login("' OR '1'='1", "x").status_code)
    case("S12", "Oversized payload (100 KB)", 413,
         lambda: httpx.post(GW + "/enrollments", content=b"{" + b" " * 100_000 + b"}",
                            headers=bearer(s1) | {"content-type": "application/json",
                                                  "idempotency-key": "k"}).status_code)
    case("S13", "Idempotency-Key reused for a different request", 422 if mode == "improved" else [201, 409],
         lambda: (lambda k: (httpx.post(GW + "/enrollments", json={"course_id": 2},
                                        headers=bearer(s1) | {"idempotency-key": k}),
                             httpx.post(GW + "/enrollments", json={"course_id": 3},
                                        headers=bearer(s1) | {"idempotency-key": k}))[1].status_code)(
             uuid.uuid4().hex), note="baseline ignores the key")
    case("S14", "Chaos endpoint without token", 403,
         lambda: httpx.post("http://127.0.0.1:8201/chaos", json={"latency_ms": 1}).status_code)
    # brute force: 5 failures allowed in 60 s, then locked (even with the right password)
    codes = [login("student0003", f"wrong{i}").status_code for i in range(7)]
    final = login("student0003", "student-pass").status_code
    case("S15", "Brute-force login (7 wrong, then correct password)", "401x5,429x2,then 429",
         lambda: f"{'401x' + str(codes.count(401))},{'429x' + str(codes.count(429))},then {final}")
    # burst from one principal: 120 requests as fast as possible
    async def burst():
        async with httpx.AsyncClient() as c:
            rs = await asyncio.gather(*[c.get(GW + "/courses", headers=bearer(s2)) for _ in range(120)])
        return sum(1 for r in rs if r.status_code == 429)
    n429 = asyncio.run(burst())
    case("S16", "Request flood from one user (120 concurrent)", "throttled" if mode == "improved" else "not throttled",
         lambda: "throttled" if n429 > 0 else "not throttled", note=f"{n429} of 120 got 429")
    return cases


async def audit_rows():
    c = await asyncpg.connect(DSN)
    rows = await c.fetch("SELECT ts, username, event, client FROM auth.audit_log ORDER BY id")
    await c.close()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", default="baseline,improved")
    a = ap.parse_args()
    for mode in a.modes.split(","):
        print(f"== E7 {mode}")
        env = Env(mode)
        env.start()
        try:
            t0 = time.time()
            cases = run_cases(mode)
            time.sleep(6)
            out = RAW / mode
            save_json(cases, out / "cases.json")
            save_json(asyncio.run(audit_rows()), out / "audit_log.json")
            save_json(prom_counters(time.time(), time.time() - t0), out / "counters.json")
            save_json(prom_alerts(t0, time.time()), out / "alerts.json")
            print(f"   passed {sum(c['pass'] for c in cases)}/{len(cases)}")
        finally:
            env.collect_logs(RAW / mode / "logs")
            env.stop()
