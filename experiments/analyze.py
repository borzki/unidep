"""Aggregate raw results into the consolidated tables, reliability metrics and charts.

    python experiments/analyze.py
Outputs: results/summary.csv, results/summary.md, results/reliability.json,
results/charts/*.png
"""
import csv
import json
import statistics as st
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "results" / "raw"
OUT = ROOT / "results"
CH = OUT / "charts"
CH.mkdir(parents=True, exist_ok=True)

C_BASE, C_IMP = "#B5523B", "#2F6DB5"
VIOL = ["duplicate_active_enrollments", "duplicate_reservations", "seat_counter_mismatch", "leaked_seats",
        "confirmed_without_seat", "acked_but_not_confirmed", "duplicate_notifications", "pending_left"]


def load(p):
    return json.loads(Path(p).read_text())


def read_csv(p):
    with open(p) as f:
        return [dict(r, t=float(r["t"]), status=int(r["status"]), latency_ms=float(r["latency_ms"]),
                     attempts=int(r["attempts"])) for r in csv.DictReader(f)]


def ms(vals, nd=1):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "n/a"
    if len(vals) == 1:
        return f"{round(vals[0], nd)}"
    return f"{round(st.mean(vals), nd)} ± {round(st.stdev(vals), nd)}"


def detection(alerts, fault_start):
    rel = [a for a in alerts if a["first_firing_s"] >= max(5, fault_start or 0) - 0.5]
    if not rel or fault_start is None:
        return None, None
    a = min(rel, key=lambda a: a["first_firing_s"])
    return round(a["first_firing_s"] - fault_start, 1), a["alert"]


def false_positives(alerts, fault_start):
    """Alerts that started firing after warm-up (5 s) but before the fault."""
    if fault_start is None:
        return [a["alert"] for a in alerts if a["first_firing_s"] >= 5]
    return [a["alert"] for a in alerts if 5 <= a["first_firing_s"] < fault_start]


def log_counts(rep_dir: Path) -> dict:
    """Exact per-run counts from the JSON service logs (Prometheus increase()
    extrapolates and can miss increments right before a process crash)."""
    out = {"retries": 0, "cb_opens": 0, "failovers": 0, "pending_recovered": 0, "security_events": 0,
           "rate_limited": 0, "crashes_injected": 0, "recovery_attempts": 0}
    rec_ids = set()
    for f in (rep_dir / "logs").glob("*.log"):
        for line in f.open(errors="ignore"):
            if not line.startswith("{"):
                continue
            if '"msg": "retrying"' in line:
                out["retries"] += 1
            elif '"msg": "circuit_breaker_transition"' in line and '"to": "open"' in line:
                out["cb_opens"] += 1
            elif '"msg": "failover"' in line:
                out["failovers"] += 1
            elif '"msg": "recovered_enrollment"' in line:
                out["recovery_attempts"] += 1
                rec_ids.add(json.loads(line).get("enrollment_id"))
            elif '"msg": "security_event"' in line:
                out["security_events"] += 1
                out["rate_limited"] += '"event": "rate_limited"' in line
            elif '"msg": "chaos_crash"' in line:
                out["crashes_injected"] += 1
    out["pending_recovered"] = len(rec_ids)
    # process-level detection by the supervisor: kill/crash -> exit observed -> respawned
    ev = rep_dir / "logs" / "supervisor_events.jsonl"
    det, rst = [], []
    if ev.exists():
        evs = [json.loads(l) for l in ev.open()]
        for i, e in enumerate(evs):
            if e["event"] == "killed":
                ex = next((x for x in evs[i:] if x["event"] == "exited" and x["service"] == e["service"]
                           and x["instance"] == e["instance"]), None)
                if ex:
                    det.append(ex["ts"] - e["ts"])
            if e["event"] == "exited":
                rs = next((x for x in evs[i:] if x["event"] == "restarted" and x["service"] == e["service"]
                           and x["instance"] == e["instance"]), None)
                if rs:
                    rst.append(rs["ts"] - e["ts"])
    out["sup_detect_s"] = round(max(det), 2) if det else None
    out["sup_respawn_s"] = round(st.mean(rst), 2) if rst else None
    out["exits"] = sum(1 for _ in rst) if rst else 0
    return out


def counter_val(c, k):
    v = c.get(k)
    if isinstance(v, dict):
        return sum(v.values())
    return v or 0


rows = []
for exp_dir in sorted(p for p in RAW.iterdir() if p.is_dir() and p.name.startswith("E") and p.name not in ("E7", "E8")):
    for mode in ("baseline", "improved"):
        reps = sorted((exp_dir / mode).glob("rep*")) if (exp_dir / mode).exists() else []
        if not reps:
            continue
        S, D, I0, I1, C, FP, EV, FF, NN = [], [], [], [], [], [], [], [], []
        plan = load(reps[0] / "plan.json")
        for r in reps:
            s = load(r / "summary.json")
            S.append(s)
            al = load(r / "alerts.json")
            D.append(detection(al, plan["fault_start"]))
            FP.extend(false_positives(al, plan["fault_start"]))
            ig = load(r / "integrity.json")
            vals = list(ig.values())
            I0.append(sum(vals[0][k] for k in VIOL))
            I1.append(sum(vals[1][k] for k in VIOL))
            n_enr = s["enroll"]["n"]
            EV.append(vals[1]["confirmed"] / n_enr * 100 if n_enr else None)
            # enrollments the client was told failed (non-2xx) but that ended CONFIRMED
            FF.append(max(0, vals[1]["confirmed"] - s["enroll"]["success"]))
            NN.append(vals[1]["confirmed_without_notification"])
            C.append(log_counts(r))
        rec = [s.get("recovery_s") for s in S]
        row = {
            "exp": exp_dir.name, "title": plan["title"], "mode": mode, "reps": len(reps),
            "success_all": ms([s["all"]["success_ratio"] * 100 for s in S], 2),
            "success_enroll": ms([s["enroll"]["success_ratio"] * 100 for s in S], 2),
            "success_during_fault": ms([s["during_fault"]["success_ratio"] * 100 for s in S if "during_fault" in s], 2),
            "enroll_eventually_confirmed": ms(EV, 2),
            "p95_ms": ms([s["all"]["p95_ms"] for s in S]),
            "p99_ms": ms([s["all"]["p99_ms"] for s in S]),
            "enroll_p99_ms": ms([s["enroll"]["p99_ms"] for s in S]),
            "max_ms": ms([s["all"]["max_ms"] for s in S]),
            "requests": ms([s["all"]["n"] for s in S], 1),
            "failed": ms([s["all"]["failed"] for s in S], 1),
            "recovery_s": "n/a (no fault)" if plan["fault_start"] is None else (
                ms([x for x in rec if x is not None]) + (
                f" ({sum(x is None for x in rec)}/{len(rec)} not recovered)" if any(x is None for x in rec) else "")),
            "supervisor_detect_s": ms([c["sup_detect_s"] for c in C], 2),
            "process_exits": ms([c["exits"] for c in C], 1),
            "detect_s": ms([d[0] for d in D if d[0] is not None]) + (
                f" [{D[0][1]}]" if D and D[0][1] else ""),
            "retries": ms([c["retries"] for c in C], 1),
            "cb_opens": ms([c["cb_opens"] for c in C], 1),
            "failovers": ms([c["failovers"] for c in C], 1),
            "recovery_actions": ms([c["pending_recovered"] for c in C], 1),
            "client_retries": ms([s.get("client_retries_used", 0) for s in S], 0),
            "integrity_violations_immediate": ms(I0, 1),
            "integrity_violations_after_quiesce": ms(I1, 1),
            "false_positive_alerts": len(FP),
            "confirmed_but_reported_failed": ms(FF, 1),
            "confirmed_without_notification": ms(NN, 1),
            "stuck_pending": ms([list(load(r / "integrity.json").values())[1]["pending_left"] for r in reps], 1),
        }
        rows.append(row)

with (OUT / "summary.csv").open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

cols = ["exp", "mode", "reps", "success_all", "success_enroll", "enroll_eventually_confirmed", "success_during_fault", "p95_ms", "p99_ms",
        "recovery_s", "detect_s", "retries", "cb_opens", "failovers", "recovery_actions", "integrity_violations_immediate",
        "integrity_violations_after_quiesce"]
md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
for r in rows:
    md.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
(OUT / "summary.md").write_text("\n".join(md) + "\n")
print("\n".join(md))


# ------------------------------------------------------------------ charts
def bins(recs, width=1.0, op=None):
    d = {}
    for r in recs:
        if op and r["op"] != op:
            continue
        b = int(r["t"] // width)
        s, n, lat = d.get(b, (0, 0, []))
        lat.append(r["latency_ms"])
        d[b] = (s + (200 <= r["status"] < 300), n + 1, lat)
    xs = sorted(d)
    return xs, [d[x][0] / d[x][1] * 100 for x in xs], [max(d[x][2]) for x in xs], [d[x][1] for x in xs]


def timeline(exp):
    fig, ax = plt.subplots(2, 1, figsize=(8, 5.2), sharex=True)
    plan = None
    for mode, col in (("baseline", C_BASE), ("improved", C_IMP)):
        p = RAW / exp / mode / "rep1" / "requests.csv"
        if not p.exists():
            continue
        plan = load(RAW / exp / mode / "rep1" / "plan.json")
        xs, ok, mx, _ = bins(read_csv(p))
        ax[0].step(xs, ok, where="post", color=col, lw=2, label=mode)
        ax[1].step(xs, [m / 1000 for m in mx], where="post", color=col, lw=2, label=mode)
    for a in ax:
        if plan and plan["fault_start"] is not None:
            a.axvspan(plan["fault_start"], plan["fault_end"], color="#999", alpha=0.15, lw=0)
        a.spines[["top", "right"]].set_visible(False)
        a.grid(axis="y", alpha=0.3)
    ax[0].set_ylabel("successful requests (%)")
    ax[0].set_ylim(-3, 103)
    ax[1].set_ylabel("max latency per 1 s (s)")
    ax[1].set_xlabel("time since start (s); shaded = fault window")
    ax[0].legend(frameon=False, loc="lower left")
    ax[0].set_title(f"{exp}: {plan['title'] if plan else ''} (rep 1)", fontsize=10, loc="left")
    fig.tight_layout()
    fig.savefig(CH / f"{exp}_timeline.png", dpi=130)
    plt.close(fig)


for e in ("E1", "E2", "E3a", "E3b", "E4", "E6"):
    if (RAW / e).exists():
        timeline(e)

# E5 / E5b load steps
for exp in ("E5", "E5b"):
    if not (RAW / exp).exists():
        continue
    fig, ax = plt.subplots(1, 2, figsize=(9, 3.6))
    mx = 0
    for mode, col in (("baseline", C_BASE), ("improved", C_IMP)):
        reps = sorted((RAW / exp / mode).glob("rep*"))
        if not reps:
            continue
        steps = load(reps[0] / "summary.json")["steps"]
        x = [int(k) for k in steps]
        mx = max(mx, max(x))
        ax[0].plot(x, [st.mean(load(r / "summary.json")["steps"][k]["throughput_ok_rps"] for r in reps)
                       for k in steps], "o-", color=col, label=mode)
        ax[1].plot(x, [st.mean(load(r / "summary.json")["steps"][k]["p95_ms"] for r in reps) for k in steps],
                   "o-", color=col, label=mode)
    ax[0].plot([0, mx], [0, mx], ls=":", color="#999", lw=1)
    ax[0].set_xlabel("offered load (req/s)")
    ax[0].set_ylabel("successful throughput (req/s)")
    ax[1].set_xlabel("offered load (req/s)")
    ax[1].set_ylabel("p95 latency (ms)")
    ax[1].set_yscale("log")
    for a in ax:
        a.spines[["top", "right"]].set_visible(False)
        a.grid(alpha=0.3)
    ax[0].legend(frameon=False)
    fig.suptitle(f"{exp}: load steps (mean of reps); dotted = ideal", fontsize=10, x=0.02, ha="left")
    fig.tight_layout()
    fig.savefig(CH / f"{exp}_load.png", dpi=130)
    plt.close(fig)


# ------------------------------------------------------------------ reliability from soak
def episodes(recs, duration, width=1.0, thr=0.95):
    """User-visible service failure episodes: maximal runs of 1-s bins whose
    success ratio is below `thr` (bins without requests count as up)."""
    d = {}
    for r in recs:
        b = int(r["t"] // width)
        s, n = d.get(b, (0, 0))
        d[b] = (s + (200 <= r["status"] < 300), n + 1)
    down = [(d.get(b, (1, 1))[0] / d.get(b, (1, 1))[1]) < thr for b in range(int(duration // width))]
    eps, cur = [], None
    for i, x in enumerate(down):
        if x and cur is None:
            cur = i
        if not x and cur is not None:
            eps.append((cur * width, (i - cur) * width))
            cur = None
    if cur is not None:
        eps.append((cur * width, (len(down) - cur) * width))
    return eps, sum(down) * width


rel = {}
for mode in ("baseline", "improved"):
    p = RAW / "R" / mode / "soak.json"
    if not p.exists():
        continue
    meta = load(p)
    recs = read_csv(RAW / "R" / mode / "requests.csv")
    T = meta["duration_s"]
    eps, downtime = episodes(recs, T)
    n = len(eps)
    uptime = T - downtime
    req_fail = sum(1 for r in recs if not (200 <= r["status"] < 300))
    repairs = [x["component_repair_s"] for x in meta["repairs"]]
    rel[mode] = {
        "observation_window_s": T, "offered_rps": meta["rps"], "requests": len(recs),
        "injected_component_faults": len(meta["fault_schedule"]),
        "component_failure_rate_per_h": round(len(meta["fault_schedule"]) / T * 3600, 1),
        "component_mttr_s_mean": round(st.mean(repairs), 2) if repairs else None,
        "component_mttr_s_sd": round(st.stdev(repairs), 2) if len(repairs) > 1 else None,
        "service_failure_episodes": n,
        "episode_durations_s": [e[1] for e in eps],
        "downtime_s": downtime, "uptime_s": uptime,
        "service_mttr_s": round(downtime / n, 2) if n else 0.0,
        "service_mttf_s": round(uptime / n, 1) if n else None,
        "service_mtbf_s": round(T / n, 1) if n else None,
        "service_failure_rate_per_h": round(n / T * 3600, 1),
        "availability_time_based": round(uptime / T, 5),
        "availability_from_mttf_mttr": round((uptime / n) / ((uptime / n) + downtime / n), 5) if n else 1.0,
        "request_success_ratio": round(1 - req_fail / len(recs), 5),
        "request_failures": req_fail,
        "integrity_ok": meta["integrity"]["integrity_ok"],
    }
if rel:
    (OUT / "reliability.json").write_text(json.dumps(rel, indent=2))
    print(json.dumps(rel, indent=2))
    fig, ax = plt.subplots(figsize=(9, 3))
    for i, (mode, col) in enumerate((("baseline", C_BASE), ("improved", C_IMP))):
        p = RAW / "R" / mode / "requests.csv"
        if p.exists():
            xs, ok, _, _ = bins(read_csv(p), width=2.0)
            ax.step([x * 2 for x in xs], ok, where="post", color=col, lw=1.4, label=mode)
    meta = load(RAW / "R" / "improved" / "soak.json") if (RAW / "R" / "improved").exists() else None
    if meta:
        for at, svc in meta["fault_schedule"]:
            ax.axvline(at, color="#666", lw=0.8, ls=":")
    ax.set_ylabel("success per 2 s (%)")
    ax.set_xlabel("time (s); dotted = injected instance crash")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, loc="lower left")
    ax.set_title("R: reliability soak, same fault schedule in both modes", fontsize=10, loc="left")
    fig.tight_layout()
    fig.savefig(CH / "R_soak.png", dpi=130)
    plt.close(fig)
