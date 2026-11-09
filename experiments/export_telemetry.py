"""Export Prometheus telemetry recorded during E6 rep1 (both profiles) as a
dashboard-style figure + CSV (evidence for monitoring/alerting)."""
import csv
import json
from pathlib import Path

import httpx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PROM = "http://127.0.0.1:9090"
Q = {
    "gateway 5xx ratio": 'sum(rate(http_requests_total{service="gateway",status=~"5.."}[15s])) / '
                         'clamp_min(sum(rate(http_requests_total{service="gateway"}[15s])),0.001)',
    "circuit breaker state (max)": "max(circuit_breaker_state)",
    "retries/s": "sum(rate(downstream_retries_total[15s]))",
    "alerts firing": 'count(ALERTS{alertstate="firing",alertname!="InstanceRestarted"}) or vector(0)',
}
out = ROOT / "results" / "telemetry"
out.mkdir(parents=True, exist_ok=True)
fig, axes = plt.subplots(len(Q), 2, figsize=(10, 7), sharex=True)
for j, mode in enumerate(("baseline", "improved")):
    plan = json.loads((ROOT / "results/raw/E6" / mode / "rep1/plan.json").read_text())
    t0 = plan["t_start_unix"]
    rows = []
    for i, (name, q) in enumerate(Q.items()):
        r = httpx.get(f"{PROM}/api/v1/query_range", params={"query": q, "start": t0, "end": t0 + 60, "step": "1s"},
                      timeout=10).json()["data"]["result"]
        xs, ys = [], []
        if r:
            for t, v in r[0]["values"]:
                xs.append(float(t) - t0)
                ys.append(float(v))
                rows.append({"metric": name, "t": round(float(t) - t0, 1), "value": v})
        ax = axes[i][j]
        ax.plot(xs, ys, color="#2F6DB5" if mode == "improved" else "#B5523B", lw=1.8)
        ax.axvspan(10, 30, color="#999", alpha=0.15, lw=0)
        ax.set_title(f"{mode}: {name}", fontsize=9, loc="left")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(alpha=0.3)
    with (out / f"E6_{mode}_rep1_prometheus.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["metric", "t", "value"])
        w.writeheader()
        w.writerows(rows)
for ax in axes[-1]:
    ax.set_xlabel("s since run start (shaded = notification-service failing)")
fig.suptitle("Prometheus telemetry exported for E6 rep 1", fontsize=10, x=0.02, ha="left")
fig.tight_layout()
fig.savefig(ROOT / "results" / "charts" / "E6_prometheus_dashboard.png", dpi=130)
print("ok")
