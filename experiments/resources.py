"""Resource cost of the improved profile: resident memory (RSS) and CPU seconds
of all service processes during 30 s of the standard 20 req/s workload.

    python experiments/resources.py
"""
import asyncio
import subprocess

from lib import ROOT, Env, LoadGen, Workload, save_json, tokens

PATTERN = "uvicorn" + " services"  # split so this script's own command line never matches


def snapshot():
    out = subprocess.run(["ps", "-eo", "rss=,times=,args="], capture_output=True, text=True).stdout.splitlines()
    procs = [line.split(None, 2) for line in out if PATTERN in line]
    rss_mb = sum(int(p[0]) for p in procs) / 1024
    cpu_s = sum(int(p[1]) for p in procs)
    return len(procs), round(rss_mb, 1), cpu_s


if __name__ == "__main__":
    res = {}
    for mode in ("baseline", "improved"):
        env = Env(mode)
        env.start()
        try:
            n0, rss0, cpu0 = snapshot()
            asyncio.run(LoadGen(Workload(rps=20, duration=30), tokens(), seed=5).run())
            n1, rss1, cpu1 = snapshot()
            res[mode] = {"processes": n1, "rss_mb_idle": rss0, "rss_mb_after_load": rss1,
                         "cpu_s_during_30s_load": cpu1 - cpu0}
            print(mode, res[mode])
        finally:
            env.stop()
    save_json(res, ROOT / "results" / "resources.json")
