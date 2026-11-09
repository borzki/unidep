"""Experiment harness: environment control, open-loop load generator, fault
injection helpers, data-integrity checks, Prometheus alert/counter capture."""
import asyncio
import csv
import json
import os
import random
import signal
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path

import asyncpg
import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common.security import issue_token  # noqa: E402

GW = os.getenv("GATEWAY_URL", "http://127.0.0.1:8000")
PROM = os.getenv("PROM_URL", "http://127.0.0.1:9090")
DSN = os.getenv("DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/unidep")
PG_CTL = os.getenv("PG_CTL", "/usr/lib/postgresql/16/bin/pg_ctl")
PGDATA = os.getenv("PGDATA_DIR", "/opt/pgdata/main")
PG_AS = os.getenv("PG_AS_USER", "pg")
CHAOS_TOKEN = os.getenv("CHAOS_TOKEN", "dev-chaos-token")
N_STUDENTS = 500
N_COURSES = 10

# ------------------------------------------------------------------ env control


class Env:
    def __init__(self, mode: str, restart: str | None = None, extra_env: dict | None = None):
        self.mode, self.restart, self.extra_env = mode, restart, extra_env or {}
        self.proc: subprocess.Popen | None = None

    def start(self):
        subprocess.run([sys.executable, "-m", "db.reset", "--mode", self.mode, "--students", str(N_STUDENTS),
                        "--courses", str(N_COURSES), "--capacity", "1000"], cwd=ROOT, check=True,
                       stdout=subprocess.DEVNULL)
        for f in (ROOT / "run" / "logs").glob("*.log") if (ROOT / "run" / "logs").exists() else []:
            f.unlink()
        (ROOT / "run" / "events.jsonl").unlink(missing_ok=True)
        cmd = [sys.executable, "deploy/local/supervisor.py", "start", "--mode", self.mode]
        if self.restart:
            cmd += ["--restart", self.restart]
        env = os.environ.copy()
        env.update(self.extra_env)
        self.proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.wait_ready()

    def wait_ready(self, timeout=30):
        t0 = time.time()
        ports = [8000, 8101, 8201, 8301, 8401] + ([8202, 8302] if self.mode == "improved" else [])
        while time.time() - t0 < timeout:
            ok = 0
            for p in ports:
                try:
                    if httpx.get(f"http://127.0.0.1:{p}/health/ready", timeout=0.5).status_code == 200:
                        ok += 1
                except httpx.HTTPError:
                    pass
            if ok == len(ports):
                time.sleep(2.5)  # let gateway health checker mark all replicas healthy, Prometheus scrape
                wait_alerts_clear()
                return
            time.sleep(0.3)
        raise RuntimeError("environment not ready")

    def stop(self):
        if self.proc:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        subprocess.run("pkill -f 'uvicorn services' ", shell=True)
        time.sleep(1.0)

    def collect_logs(self, dest: Path):
        dest.mkdir(parents=True, exist_ok=True)
        logs = ROOT / "run" / "logs"
        if logs.exists():
            for f in logs.glob("*.log"):
                (dest / f.name).write_bytes(f.read_bytes())
        ev = ROOT / "run" / "events.jsonl"
        if ev.exists():
            (dest / "supervisor_events.jsonl").write_bytes(ev.read_bytes())


def wait_alerts_clear(timeout=60):
    """Do not start measuring while alerts from a previous run are still active."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = httpx.get(f"{PROM}/api/v1/alerts", timeout=2).json()["data"]["alerts"]
            if not [a for a in r if a["state"] in ("firing", "pending") and a["labels"].get("alertname") != "InstanceRestarted"]:
                return
        except Exception:  # noqa: BLE001
            return
        time.sleep(1)


def kill(service: str, instance: int):
    subprocess.run([sys.executable, "deploy/local/supervisor.py", "kill", service, str(instance)], cwd=ROOT,
                   check=True, stdout=subprocess.DEVNULL)


def pg(action: str):
    """stop (immediate = crash-like) / start the PostgreSQL server."""
    if action == "stop":
        cmd = f"{PG_CTL} -D {PGDATA} -m immediate stop"
    else:
        cmd = f"{PG_CTL} -D {PGDATA} -o '-p 5432 -k /tmp' -l {PGDATA}/../log.txt -w start"
    subprocess.run(["su", PG_AS, "-c", cmd], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def chaos(port: int, **state):
    r = httpx.post(f"http://127.0.0.1:{port}/chaos", json=state, headers={"x-chaos-token": CHAOS_TOKEN}, timeout=2)
    r.raise_for_status()


def tokens() -> dict[int, str]:
    # synthetic test principals; ids 2..N+1 are the seeded students (id 1 = admin)
    return {sid: issue_token(sid, f"student{sid - 1:04d}", "student", ttl=7200) for sid in range(2, N_STUDENTS + 2)}


# ------------------------------------------------------------------ load gen


@dataclass
class Rec:
    t: float
    op: str
    status: int
    first_status: int
    latency_ms: float
    attempts: int
    err: str = ""
    served_by: str = ""
    enrollment_id: str = ""
    student: int = 0
    course: int = 0
    key: str = ""


@dataclass
class Workload:
    rps: float = 20.0
    duration: float = 40.0
    read_ratio: float = 0.7
    client_retries: int = 0          # client re-sends with the SAME Idempotency-Key
    client_retry_delay: float = 1.0
    client_timeout: float = 35.0
    rps_steps: list = field(default_factory=list)  # [(start_s, rps), ...] overrides rps


class LoadGen:
    def __init__(self, wl: Workload, toks: dict[int, str], seed: int = 1):
        self.wl, self.toks = wl, toks
        rnd = random.Random(seed)
        self.pairs = [(s, c) for s in toks for c in range(1, N_COURSES + 1)]
        rnd.shuffle(self.pairs)
        self.rnd = rnd
        self.recs: list[Rec] = []
        self.t0 = 0.0

    async def _enroll(self, client, t):
        sid, cid = self.pairs.pop()
        key = uuid.uuid4().hex
        h = {"authorization": f"Bearer {self.toks[sid]}", "idempotency-key": key, "x-request-id": key[:16]}
        start = time.perf_counter()
        first, status, err, served, eid, attempts = None, 0, "", "", "", 0
        for attempt in range(1 + self.wl.client_retries):
            attempts += 1
            try:
                r = await client.post(f"{GW}/enrollments", json={"course_id": cid}, headers=h)
                status, err = r.status_code, ""
                served = r.headers.get("x-served-by", "")
                try:
                    eid = r.json().get("id", "") or eid
                except Exception:  # noqa: BLE001
                    pass
            except httpx.HTTPError as exc:
                status, err = 0, type(exc).__name__
            if first is None:
                first = status
            if status in (0, 500, 502, 503, 504) and attempt < self.wl.client_retries:
                await asyncio.sleep(self.wl.client_retry_delay)
                continue
            break
        self.recs.append(Rec(t, "enroll", status, first, (time.perf_counter() - start) * 1000, attempts, err,
                             served, eid, sid, cid, key))

    async def _read(self, client, t):
        sid = self.rnd.choice(list(self.toks))
        start = time.perf_counter()
        try:
            r = await client.get(f"{GW}/courses", headers={"authorization": f"Bearer {self.toks[sid]}"})
            status, err = r.status_code, ""
        except httpx.HTTPError as exc:
            status, err = 0, type(exc).__name__
        self.recs.append(Rec(t, "read", status, status, (time.perf_counter() - start) * 1000, 1, err, "", "", sid))

    def _rate_at(self, t):
        rate = self.wl.rps
        for start, r in self.wl.rps_steps:
            if t >= start:
                rate = r
        return rate

    async def run(self, events: list | None = None):
        """events: [(at_seconds, callable)] executed in a thread at the given offset."""
        limits = httpx.Limits(max_connections=2000, max_keepalive_connections=200)
        async with httpx.AsyncClient(timeout=self.wl.client_timeout, limits=limits) as client:
            self.t0 = time.time()
            loop = asyncio.get_running_loop()
            pending = sorted(events or [], key=lambda e: e[0])
            tasks = []
            t = 0.0
            while t < self.wl.duration:
                now = time.time() - self.t0
                while pending and pending[0][0] <= now:
                    at, fn = pending.pop(0)
                    tasks.append(loop.run_in_executor(None, fn))
                if t > now:
                    await asyncio.sleep(t - now)
                if self.rnd.random() < self.wl.read_ratio:
                    tasks.append(asyncio.create_task(self._read(client, t)))
                else:
                    tasks.append(asyncio.create_task(self._enroll(client, t)))
                t += 1.0 / self._rate_at(t)
            for at, fn in pending:  # events scheduled after the load window
                await asyncio.sleep(max(0, at - (time.time() - self.t0)))
                tasks.append(loop.run_in_executor(None, fn))
            await asyncio.gather(*tasks)
        return self.recs


def write_csv(recs: list[Rec], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(recs[0]).keys()))
        w.writeheader()
        for r in sorted(recs, key=lambda r: r.t):
            w.writerow(asdict(r))


# ------------------------------------------------------------------ analysis


def pct(values, q):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(q / 100 * (len(s) - 1)))))
    return round(s[k], 1)


def summarize(recs: list[Rec], fault_start: float | None = None, fault_end: float | None = None,
              ok=lambda r: 200 <= r.status < 300) -> dict:
    def block(rs):
        n = len(rs)
        succ = sum(1 for r in rs if ok(r))
        lat = [r.latency_ms for r in rs]
        return {"n": n, "success": succ, "failed": n - succ,
                "success_ratio": round(succ / n, 4) if n else None,
                "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95), "p99_ms": pct(lat, 99),
                "max_ms": round(max(lat), 1) if lat else None,
                "status_counts": {str(k): v for k, v in sorted(_count(r.status for r in rs).items())}}
    out = {"all": block(recs), "enroll": block([r for r in recs if r.op == "enroll"]),
           "read": block([r for r in recs if r.op == "read"])}
    if fault_start is not None:
        fe = fault_end if fault_end is not None else max(r.t for r in recs)
        out["during_fault"] = block([r for r in recs if fault_start <= r.t < fe])
        out["recovery_s"] = recovery_time(recs, fault_start, ok)
        out["client_retries_used"] = sum(r.attempts - 1 for r in recs)
    return out


def _count(it):
    d = {}
    for x in it:
        d[x] = d.get(x, 0) + 1
    return d


def recovery_time(recs, fault_start, ok, bin_s=1.0, need=3, threshold=0.95):
    """Seconds from fault injection until the first of `need` consecutive 1-s
    bins (by request send time) with success ratio >= threshold. None = not
    recovered inside the observation window (right-censored)."""
    bins = {}
    for r in recs:
        if r.t >= fault_start:
            b = int((r.t - fault_start) // bin_s)
            s, n = bins.get(b, (0, 0))
            bins[b] = (s + (1 if ok(r) else 0), n + 1)
    if not bins:
        return None
    last = max(bins)
    run = 0
    first_bad_seen = False
    for b in range(0, last + 1):
        s, n = bins.get(b, (0, 0))
        good = n > 0 and s / n >= threshold
        if not good:
            first_bad_seen = True
            run = 0
            continue
        run += 1
        if run >= need:
            start_bin = b - need + 1
            return round(start_bin * bin_s, 1) if first_bad_seen else 0.0
    return None


async def integrity(acked_eids: list[str]) -> dict:
    """Data-integrity checks run after every experiment."""
    c = await asyncpg.connect(DSN)
    q = {
        "enrollments_total": "SELECT count(*) FROM enrollment.enrollments",
        "confirmed": "SELECT count(*) FROM enrollment.enrollments WHERE status='CONFIRMED'",
        "pending_left": "SELECT count(*) FROM enrollment.enrollments WHERE status='PENDING'",
        "failed": "SELECT count(*) FROM enrollment.enrollments WHERE status='FAILED'",
        "duplicate_active_enrollments": """SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM enrollment.enrollments
              WHERE status IN ('PENDING','CONFIRMED') GROUP BY student_id, course_id HAVING count(*)>1) x""",
        "reservations_total": "SELECT count(*) FROM course.reservations",
        "duplicate_reservations": """SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM course.reservations
              GROUP BY enrollment_id HAVING count(*)>1) x""",
        "seat_counter_mismatch": """SELECT coalesce(sum(abs(c.seats_taken - coalesce(r.n,0))),0) FROM course.courses c
              LEFT JOIN (SELECT course_id, count(*) n FROM course.reservations GROUP BY course_id) r ON r.course_id=c.id""",
        "leaked_seats": """SELECT count(*) FROM course.reservations r LEFT JOIN enrollment.enrollments e
              ON e.id=r.enrollment_id WHERE e.id IS NULL OR e.status NOT IN ('CONFIRMED','PENDING')""",
        "confirmed_without_seat": """SELECT count(*) FROM enrollment.enrollments e LEFT JOIN course.reservations r
              ON r.enrollment_id=e.id WHERE e.status='CONFIRMED' AND r.id IS NULL""",
        "confirmed_without_notification": """SELECT count(*) FROM enrollment.enrollments e LEFT JOIN
              notification.notifications n ON n.enrollment_id=e.id WHERE e.status='CONFIRMED' AND n.id IS NULL""",
        "duplicate_notifications": """SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM notification.notifications
              GROUP BY enrollment_id HAVING count(*)>1) x""",
    }
    out = {k: int(await c.fetchval(v)) for k, v in q.items()}
    acked = [uuid.UUID(e) for e in acked_eids if e]
    if acked:
        rows = await c.fetch("SELECT id, status FROM enrollment.enrollments WHERE id = ANY($1::uuid[])", acked)
        st = {r["id"]: r["status"] for r in rows}
        out["acked_but_not_confirmed"] = sum(1 for e in acked if st.get(e) != "CONFIRMED")
    else:
        out["acked_but_not_confirmed"] = 0
    await c.close()
    keys = ["duplicate_active_enrollments", "duplicate_reservations", "seat_counter_mismatch", "leaked_seats",
            "confirmed_without_seat", "acked_but_not_confirmed", "duplicate_notifications", "pending_left"]
    out["integrity_ok"] = all(out[k] == 0 for k in keys)
    return out


def wait_quiesce(seconds: float):
    time.sleep(seconds)


def prom_alerts(t_start: float, t_end: float) -> list[dict]:
    """First firing time (relative to t_start) for each alert in the window."""
    try:
        r = httpx.get(f"{PROM}/api/v1/query_range", params={
            "query": 'ALERTS{alertstate="firing"}', "start": t_start, "end": t_end, "step": "1s"}, timeout=10)
        res = r.json()["data"]["result"]
    except Exception:  # noqa: BLE001
        return []
    out = []
    for s in res:
        m = s["metric"]
        first = float(s["values"][0][0])
        out.append({"alert": m.get("alertname"), "labels": {k: v for k, v in m.items()
                    if k not in ("__name__", "alertname", "alertstate")},
                    "first_firing_s": round(first - t_start, 1),
                    "last_firing_s": round(float(s["values"][-1][0]) - t_start, 1)})
    return sorted(out, key=lambda a: a["first_firing_s"])


COUNTERS = {
    "retries": "sum(increase(downstream_retries_total[{w}s]))",
    "circuit_opens": "sum(increase(circuit_breaker_open_total[{w}s]))",
    "gateway_failovers": "sum(increase(gateway_failover_total[{w}s]))",
    "recovery_actions": "sum by (action) (increase(recovery_actions_total[{w}s]))",
    "security_events": "sum by (event) (increase(security_events_total[{w}s]))",
    "rate_limited": "sum(increase(gateway_rate_limited_total[{w}s]))",
}


def prom_counters(t_end: float, window: float) -> dict:
    """Counter increases over the window. NOTE: counters reset when a process
    restarts, Prometheus increase() compensates for resets between scrapes but
    increments made in the last <2 s before a crash may be missed."""
    out = {}
    for name, q in COUNTERS.items():
        try:
            r = httpx.get(f"{PROM}/api/v1/query", params={"query": q.format(w=int(window) + 4), "time": t_end},
                          timeout=10).json()["data"]["result"]
            if len(r) == 1 and not r[0]["metric"]:
                out[name] = round(float(r[0]["value"][1]), 1)
            else:
                out[name] = {next(iter(x["metric"].values()), "?"): round(float(x["value"][1]), 1) for x in r}
        except Exception:  # noqa: BLE001
            out[name] = None
    return out


def save_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str))
