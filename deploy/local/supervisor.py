"""Local process orchestrator (used when containers are not available).

Mirrors docker-compose semantics that matter for the experiments:
  * replicas per service (baseline: 1, improved: 2 for course/enrollment)
  * restart policy (baseline: none, improved: restart on exit after 1 s,
    like `restart: unless-stopped`)
  * Prometheus file-based service discovery for the running replicas

Usage:
  python deploy/local/supervisor.py start --mode improved   # foreground
  python deploy/local/supervisor.py kill enrollment 0       # SIGKILL a replica
  python deploy/local/supervisor.py status
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN = ROOT / "run"
STATE = RUN / "state.json"
EVENTS = RUN / "events.jsonl"
LOGS = RUN / "logs"

TOPOLOGY = {
    "improved": {"auth": [8101], "course": [8201, 8202], "enrollment": [8301, 8302],
                 "notification": [8401], "gateway": [8000]},
    "baseline": {"auth": [8101], "course": [8201], "enrollment": [8301],
                 "notification": [8401], "gateway": [8000]},
}
ORDER = ["auth", "course", "notification", "enrollment", "gateway"]


def event(kind: str, **kw):
    RUN.mkdir(exist_ok=True)
    with EVENTS.open("a") as f:
        f.write(json.dumps({"ts": time.time(), "event": kind, **kw}) + "\n")


def env_for(mode: str, svc: str, idx: int, topo: dict) -> dict:
    e = os.environ.copy()
    url = lambda name: ",".join(f"http://127.0.0.1:{p}" for p in topo[name])  # noqa: E731
    e.update({
        "RESILIENCE_MODE": mode, "SERVICE_NAME": svc, "INSTANCE_ID": str(idx),
        "CHAOS_ENABLED": "1", "PYTHONPATH": str(ROOT),
        "COURSE_URLS": url("course"), "NOTIFICATION_URLS": url("notification"),
        "UPSTREAMS": ";".join(f"{n}={url(n)}" for n in ("auth", "course", "enrollment", "notification")),
    })
    return e


def spawn(mode, svc, idx, port, topo):
    LOGS.mkdir(parents=True, exist_ok=True)
    log = open(LOGS / f"{svc}-{idx}.log", "a")
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", f"services.{svc}.app:app", "--host", "127.0.0.1",
         "--port", str(port), "--no-access-log", "--log-level", "warning"],
        cwd=ROOT, env=env_for(mode, svc, idx, topo), stdout=log, stderr=subprocess.STDOUT)
    return p


def write_sd(topo):
    targets = [{"targets": [f"127.0.0.1:{p}" for p in ports], "labels": {"job": svc}}
               for svc, ports in topo.items()]
    (RUN / "sd.json").write_text(json.dumps(targets))


def cmd_start(mode: str, restart: bool | None):
    topo = TOPOLOGY[mode]
    restart = (mode == "improved") if restart is None else restart
    RUN.mkdir(exist_ok=True)
    write_sd(topo)
    procs = {}
    for svc in ORDER:
        for idx, port in enumerate(topo[svc]):
            procs[(svc, idx)] = (spawn(mode, svc, idx, port, topo), port)
        time.sleep(0.3)
    event("started", mode=mode, restart=restart)
    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while not stopping:
        STATE.write_text(json.dumps({"mode": mode, "restart": restart, "supervisor": os.getpid(),
                                     "procs": {f"{s}:{i}": {"pid": p.pid, "port": port, "alive": p.poll() is None}
                                               for (s, i), (p, port) in procs.items()}}))
        for (svc, idx), (p, port) in list(procs.items()):
            rc = p.poll()
            if rc is not None and not getattr(p, "_reported", False):
                p._reported = True
                event("exited", service=svc, instance=idx, code=rc)
                if restart:
                    time.sleep(1.0)  # restart delay
                    procs[(svc, idx)] = (spawn(mode, svc, idx, port, topo), port)
                    event("restarted", service=svc, instance=idx)
        time.sleep(0.2)
    for p, _ in procs.values():
        if p.poll() is None:
            p.terminate()
    for p, _ in procs.values():
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()
    event("stopped")
    STATE.unlink(missing_ok=True)


def cmd_kill(svc: str, idx: int):
    st = json.loads(STATE.read_text())
    pid = st["procs"][f"{svc}:{idx}"]["pid"]
    os.kill(pid, signal.SIGKILL)
    event("killed", service=svc, instance=idx, pid=pid)
    print(f"killed {svc}:{idx} pid={pid}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start")
    s.add_argument("--mode", choices=["baseline", "improved"], default="improved")
    s.add_argument("--restart", choices=["on", "off"], default=None)
    k = sub.add_parser("kill")
    k.add_argument("service")
    k.add_argument("instance", type=int)
    sub.add_parser("status")
    a = ap.parse_args()
    if a.cmd == "start":
        cmd_start(a.mode, None if a.restart is None else a.restart == "on")
    elif a.cmd == "kill":
        cmd_kill(a.service, a.instance)
    else:
        print(STATE.read_text() if STATE.exists() else "not running")
