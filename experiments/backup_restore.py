"""E8 - recovery / rollback verification.

Part A (backup & restore, infrastructure mechanism): under load, take a
pg_dump at t=15 s; at t=30 s simulate a destructive operator error (enrollment
and reservation data deleted). Improved: restore from the dump and verify the
acceptance criteria; measure RTO and RPO. Baseline: no backup exists.

Part B (compensation / rollback, software mechanism): cancel confirmed
enrollments and verify every seat is released and counters stay consistent.

    python experiments/backup_restore.py --reps 3
"""
import argparse
import asyncio
import subprocess
import time
import uuid

import asyncpg
import httpx

from lib import DSN, GW, ROOT, Env, LoadGen, Workload, integrity, save_json, tokens, write_csv

RAW = ROOT / "results" / "raw" / "E8"
BIN = "/usr/lib/postgresql/16/bin"
DUMP = "/tmp/unidep_backup.dump"


async def q(sql):
    c = await asyncpg.connect(DSN)
    v = await c.fetchval(sql)
    await c.close()
    return v


async def destroy():
    c = await asyncpg.connect(DSN)
    async with c.transaction():
        await c.execute("DELETE FROM notification.notifications")
        await c.execute("DELETE FROM enrollment.outbox")
        await c.execute("DELETE FROM enrollment.enrollments")
        await c.execute("DELETE FROM course.reservations")
        await c.execute("UPDATE course.courses SET seats_taken = 0")
    await c.close()


def backup() -> float:
    t = time.perf_counter()
    subprocess.run([f"{BIN}/pg_dump", "-h", "127.0.0.1", "-U", "postgres", "-Fc", "-f", DUMP, "unidep"], check=True)
    return time.perf_counter() - t


def restore():
    subprocess.run([f"{BIN}/pg_restore", "-h", "127.0.0.1", "-U", "postgres", "--clean", "--if-exists",
                    "--single-transaction", "-d", "unidep", DUMP], check=True)


def acceptance_probe(toks, n=20) -> dict:
    """AC: workflow works end-to-end after recovery (n fresh enrollments)."""
    ok = 0
    sids = list(toks)[-n:]
    for sid in sids:
        r = httpx.post(GW + "/enrollments", json={"course_id": 10},
                       headers={"authorization": f"Bearer {toks[sid]}", "idempotency-key": uuid.uuid4().hex})
        ok += r.status_code in (200, 201)
    return {"probe_requests": n, "probe_success": ok}


def part_a(mode: str, rep: int):
    out = RAW / "backup_restore" / mode / f"rep{rep}"
    env = Env(mode)
    env.start()
    info = {"mode": mode, "rep": rep}
    try:
        toks = tokens()
        marks = {}

        def do_backup():
            marks["backup_t"] = time.time()
            if mode == "improved":
                marks["backup_duration_s"] = round(backup(), 3)
            marks["confirmed_at_backup"] = asyncio.run(q(
                "SELECT count(*) FROM enrollment.enrollments WHERE status='CONFIRMED'"))
        lg = LoadGen(Workload(rps=20, duration=30, read_ratio=0.5), toks, seed=100 + rep)
        recs = asyncio.run(lg.run([(15, do_backup)]))
        write_csv(recs, out / "requests.csv")
        time.sleep(3)
        confirmed_before = asyncio.run(q("SELECT count(*) FROM enrollment.enrollments WHERE status='CONFIRMED'"))
        t_fault = time.time()
        asyncio.run(destroy())
        info["confirmed_before_incident"] = confirmed_before
        info["confirmed_after_incident"] = asyncio.run(q(
            "SELECT count(*) FROM enrollment.enrollments WHERE status='CONFIRMED'"))
        info.update(marks)
        if mode == "improved":
            t_r = time.time()
            restore()
            info["restore_duration_s"] = round(time.time() - t_r, 3)
            info["confirmed_after_restore"] = asyncio.run(q(
                "SELECT count(*) FROM enrollment.enrollments WHERE status='CONFIRMED'"))
            info["rpo_records_lost"] = confirmed_before - info["confirmed_after_restore"]
            info["rpo_seconds"] = round(t_fault - marks["backup_t"], 1)
            # restored state predates later requests; recovery loop must settle any PENDING rows
            time.sleep(8)
            info["integrity_after_restore"] = asyncio.run(integrity([]))
            info["rto_s"] = round(time.time() - t_fault, 1)
            info["rto_note"] = "fault -> restore complete -> integrity verified (includes 8 s settle wait)"
        else:
            info["rpo_records_lost"] = confirmed_before - info["confirmed_after_incident"]
            info["integrity_after_incident"] = asyncio.run(integrity([]))
            info["rto_s"] = None
            info["rto_note"] = "no backup: data not recoverable"
        info.update(acceptance_probe(toks))
        print(f"   {mode} rep{rep}: {({k: v for k, v in info.items() if 'integrity' not in k})}")
        save_json(info, out / "result.json")
    finally:
        env.collect_logs(out / "logs")
        env.stop()


def part_b(mode: str):
    out = RAW / "compensation" / mode
    env = Env(mode)
    env.start()
    try:
        toks = tokens()
        sids = list(toks)[:40]
        eids = []
        for sid in sids:
            r = httpx.post(GW + "/enrollments", json={"course_id": 7},
                           headers={"authorization": f"Bearer {toks[sid]}", "idempotency-key": uuid.uuid4().hex})
            if r.status_code in (200, 201):
                eids.append((sid, r.json()["id"]))
        before = asyncio.run(q("SELECT seats_taken FROM course.courses WHERE id=7"))
        cancelled = 0
        for sid, eid in eids[:20]:
            r = httpx.post(f"{GW}/enrollments/{eid}/cancel", headers={"authorization": f"Bearer {toks[sid]}"})
            cancelled += r.status_code == 200
        # cancelling twice must be harmless (idempotent compensation)
        for sid, eid in eids[:5]:
            httpx.post(f"{GW}/enrollments/{eid}/cancel", headers={"authorization": f"Bearer {toks[sid]}"})
        after = asyncio.run(q("SELECT seats_taken FROM course.courses WHERE id=7"))
        res = {"mode": mode, "enrolled": len(eids), "seats_before": before, "cancelled": cancelled,
               "seats_after": after, "expected_seats_after": before - cancelled,
               "integrity": asyncio.run(integrity([]))}
        res["pass"] = after == before - cancelled and res["integrity"]["integrity_ok"]
        print(f"   compensation {mode}: seats {before}->{after} (cancelled {cancelled}) pass={res['pass']}")
        save_json(res, out / "result.json")
    finally:
        env.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()
    for rep in range(1, a.reps + 1):
        for m in ("baseline", "improved"):
            part_a(m, rep)
    for m in ("baseline", "improved"):
        part_b(m)
