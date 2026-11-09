"""Fault-injection experiments E1-E6 (baseline vs improved).

    python experiments/run.py E1 --reps 3            # one experiment, both modes
    python experiments/run.py all --reps 3
Each run writes results/raw/<exp>/<mode>/rep<i>/ (requests.csv, summary.json,
integrity.json, alerts.json, counters.json, plan.json, logs/).
"""
import argparse
import asyncio
import json
import time
from pathlib import Path

from lib import (ROOT, Env, LoadGen, Workload, chaos, integrity, kill, pg, prom_alerts, prom_counters,
                 save_json, summarize, tokens, write_csv)

RAW = ROOT / "results" / "raw"

COURSE_PORTS = {"baseline": [8201], "improved": [8201, 8202]}
ENROLL_PORTS = {"baseline": [8301], "improved": [8301, 8302]}


def _safe(fn):
    def w():
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            print("   event failed:", exc)
    return w


def plan(exp: str, mode: str) -> dict:
    """Pre-registered plan: hypothesis, workload, fault, window, success criterion."""
    if exp == "E1":
        return dict(
            title="Application/service instance failure",
            hypothesis="With 2 replicas, gateway health-check ejection/failover and restart policy, the enrollment "
                       "workflow stays >=99% successful when one enrollment and one course instance are SIGKILLed; "
                       "the single-instance baseline loses the workflow until manual intervention.",
            workload=Workload(rps=20, duration=40), fault_start=10, fault_end=40,
            events=[(10, lambda: kill("enrollment", 0)), (10, lambda: kill("course", 0))],
            fault="SIGKILL enrollment:0 and course:0 at t=10 s", success="HTTP 2xx")
    if exp == "E2":
        return dict(
            title="Database unavailable",
            hypothesis="During a 10 s PostgreSQL outage requests fail fast (no hang, no partial writes); after the "
                       "DB returns the system recovers automatically within 5 s with zero integrity violations.",
            workload=Workload(rps=20, duration=40), fault_start=10, fault_end=20,
            events=[(10, lambda: pg("stop")), (20, lambda: pg("start"))],
            fault="pg_ctl stop -m immediate at t=10 s, start at t=20 s", success="HTTP 2xx")
    if exp == "E3a":
        ports = COURSE_PORTS[mode]
        return dict(
            title="Network delay on course-service (3 s added latency)",
            hypothesis="Tight timeouts + bounded retries + circuit breaker keep enrollment latency bounded "
                       "(p99 < 5 s) while the dependency is slow; baseline latency follows the injected delay.",
            workload=Workload(rps=20, duration=40), fault_start=10, fault_end=25,
            events=[(10, lambda: [chaos(p, latency_ms=3000, path_prefix="/internal") for p in ports]),
                    (25, lambda: [chaos(p, latency_ms=0, path_prefix="/internal") for p in ports])],
            fault="+3000 ms on course-service /internal/* from t=10 to t=25 s", success="HTTP 2xx")
    if exp == "E3b":
        ports = COURSE_PORTS[mode]
        return dict(
            title="Transient dependency errors (30% of course-service calls return 503)",
            hypothesis="Bounded retries with backoff mask transient 30% failures (enroll success >= 97%); "
                       "baseline success drops to ~70% for enrollment.",
            workload=Workload(rps=20, duration=40), fault_start=10, fault_end=30,
            events=[(10, lambda: [chaos(p, error_rate=0.3, path_prefix="/internal") for p in ports]),
                    (30, lambda: [chaos(p, error_rate=0.0, path_prefix="/internal") for p in ports])],
            fault="30% injected 503 on course-service /internal/* from t=10 to t=30 s", success="HTTP 2xx")
    if exp == "E4":
        def crash():
            for p in ENROLL_PORTS[mode][:1]:
                chaos(p, crash_point="after_reserve")
        return dict(
            title="Interrupted multi-step transaction (crash after seat reservation)",
            hypothesis="With idempotency keys + checkpoint recovery, a crash between 'seat reserved' and "
                       "'enrollment confirmed' produces no duplicate enrollments, no leaked seats and no lost "
                       "acknowledged enrollments; baseline produces duplicates/orphans.",
            workload=Workload(rps=20, duration=40, client_retries=1, client_retry_delay=1.0),
            fault_start=10, fault_end=40, restart="on",
            events=[(10, _safe(crash)), (20, _safe(crash)), (30, _safe(crash))],
            fault="enrollment:0 hard-exits (os._exit) after reserving a seat, armed at t=10, 20, 30 s; "
                  "restart policy ON in both modes to isolate the transaction mechanisms; client retries once "
                  "with the same Idempotency-Key", success="HTTP 2xx")
    if exp == "E5":
        return dict(
            title="High load / traffic spike",
            hypothesis="Throughput tracks offered load up to saturation; improved keeps error ratio < 1% at "
                       "<= 50 rps; beyond saturation latency grows and errors appear.",
            workload=Workload(rps=20, duration=80, rps_steps=[(0, 20), (20, 50), (40, 80), (60, 120)]),
            fault_start=None, fault_end=None, events=[],
            fault="offered load steps 20 -> 50 -> 80 -> 120 req/s, 20 s each", success="HTTP 2xx")
    if exp == "E6":
        return dict(
            title="Monitoring and alert trigger (notification-service failing)",
            hypothesis="A failing notification dependency is detected by an alert within 10 s and the failing "
                       "request can be traced across services by request id; improved degrades gracefully "
                       "(outbox) so users still enroll.",
            workload=Workload(rps=20, duration=45), fault_start=10, fault_end=30,
            events=[(10, lambda: chaos(8401, error_rate=1.0, path_prefix="/internal")),
                    (30, lambda: chaos(8401, error_rate=0.0, path_prefix="/internal"))],
            fault="100% 503 from notification-service /internal/* t=10..30 s", success="HTTP 2xx")
    raise ValueError(exp)


def run_one(exp: str, mode: str, rep: int, quiesce: float = 15.0):
    p = plan(exp, mode)
    out = RAW / exp / mode / f"rep{rep}"
    env = Env(mode, restart=p.get("restart"))
    print(f"== {exp} {mode} rep{rep}: {p['title']}")
    env.start()
    try:
        toks = tokens()
        lg = LoadGen(p["workload"], toks, seed=rep)
        t_start = time.time()
        recs = asyncio.run(lg.run(p["events"]))
        t_load_end = time.time()
        acked = [r.enrollment_id for r in recs if r.op == "enroll" and 200 <= r.status < 300]
        integ_now = asyncio.run(integrity(acked))
        time.sleep(quiesce)  # allow background recovery / outbox to finish
        integ_after = asyncio.run(integrity(acked))
        t_end = time.time()
        summ = summarize(recs, p["fault_start"], p["fault_end"])
        if exp == "E5":
            summ["steps"] = {}
            for (s, rate), nxt in zip(p["workload"].rps_steps, p["workload"].rps_steps[1:] + [(80, None)]):
                block = [r for r in recs if s <= r.t < nxt[0]]
                b = summarize(block)["all"]
                b["offered_rps"] = rate
                b["throughput_ok_rps"] = round(b["success"] / (nxt[0] - s), 1)
                summ["steps"][str(rate)] = b
        save_json({k: (v if k not in ("events", "workload") else (str(v) if k == "workload" else len(v)))
                   for k, v in p.items()} | {"mode": mode, "rep": rep, "t_start_unix": t_start}, out / "plan.json")
        write_csv(recs, out / "requests.csv")
        save_json(summ, out / "summary.json")
        save_json({"immediately_after_load": integ_now, f"after_{int(quiesce)}s_quiesce": integ_after},
                  out / "integrity.json")
        save_json(prom_alerts(t_start, t_end), out / "alerts.json")
        save_json(prom_counters(t_load_end, t_load_end - t_start), out / "counters.json")
        a = summ["all"]
        print(f"   success={a['success_ratio']} enroll={summ['enroll']['success_ratio']} p95={a['p95_ms']} "
              f"p99={a['p99_ms']} rec={summ.get('recovery_s')} integrity_now={integ_now['integrity_ok']} "
              f"after={integ_after['integrity_ok']}")
    finally:
        env.collect_logs(out / "logs")
        env.stop()
        if exp == "E2":
            try:
                pg("start")
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("exp")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--modes", default="baseline,improved")
    a = ap.parse_args()
    exps = ["E1", "E2", "E3a", "E3b", "E4", "E6", "E5"] if a.exp == "all" else a.exp.split(",")
    for e in exps:
        reps = min(a.reps, 2) if e == "E5" else a.reps
        for rep in range(1, reps + 1):
            for m in a.modes.split(","):
                run_one(e, m, rep)
