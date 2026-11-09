"""R - reliability soak used for the MTTF/MTBF/MTTR/availability calculations.

Constant load (10 req/s) for DURATION seconds while crash faults (SIGKILL of
an application instance) are injected at exponentially distributed intervals
(mean 40 s, fixed seed => identical fault schedule in both modes).
The platform restart policy is ON in both modes so that the baseline also has
finite repair times; the comparison therefore isolates replication, gateway
failover, retries, circuit breaking and idempotent recovery.

    python experiments/soak.py --duration 300
"""
import argparse
import asyncio
import json
import random
import time

import httpx

from lib import ROOT, Env, LoadGen, Workload, integrity, kill, save_json, tokens, write_csv

RAW = ROOT / "results" / "raw" / "R"
PORTS = {("course", 0): 8201, ("course", 1): 8202, ("enrollment", 0): 8301, ("enrollment", 1): 8302}


def schedule(duration, mean, seed):
    rnd = random.Random(seed)
    t, out = 10.0, []
    while True:
        t += rnd.expovariate(1 / mean)
        if t > duration - 15:
            return out
        out.append((round(t, 1), rnd.choice(["course", "enrollment"])))


def run(mode: str, duration: float, mean: float, seed: int):
    out = RAW / mode
    env = Env(mode, restart="on")
    env.start()
    repairs = []
    try:
        toks = tokens()
        sched = schedule(duration, mean, seed)

        def make(svc, at):
            def f():
                inst = 0
                port = PORTS[(svc, inst)]
                t_kill = time.time()
                kill(svc, inst)
                # component repair time: kill -> readiness probe OK again
                while time.time() - t_kill < 60:
                    try:
                        if httpx.get(f"http://127.0.0.1:{port}/health/ready", timeout=0.3).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.1)
                repairs.append({"at_s": at, "service": svc, "instance": inst,
                                "component_repair_s": round(time.time() - t_kill, 2)})
            return f
        events = [(at, make(svc, at)) for at, svc in sched]
        lg = LoadGen(Workload(rps=10, duration=duration, client_retries=0), toks, seed=seed)
        recs = asyncio.run(lg.run(events))
        acked = [r.enrollment_id for r in recs if r.op == "enroll" and 200 <= r.status < 300]
        time.sleep(15)
        integ = asyncio.run(integrity(acked))
        write_csv(recs, out / "requests.csv")
        save_json({"mode": mode, "duration_s": duration, "rps": 10, "mean_interfault_s": mean, "seed": seed,
                   "fault_schedule": sched, "repairs": repairs, "integrity": integ}, out / "soak.json")
        print(f"   {mode}: faults={len(sched)} repairs={[r['component_repair_s'] for r in repairs]} "
              f"integrity={integ['integrity_ok']}")
    finally:
        env.collect_logs(out / "logs")
        env.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--mean", type=float, default=40)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    for m in ("baseline", "improved"):
        print(f"== R soak {m}")
        run(m, a.duration, a.mean, a.seed)
