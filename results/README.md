# Results

- `summary.csv` / `summary.md`: consolidated table for E1-E6 (mean ± sd over repetitions)
- `reliability.json`: MTTF/MTBF/MTTR/availability computed from the soak (R)
- `charts/`: timelines (rep 1), load chart, soak chart, exported Prometheus telemetry
- `telemetry/`: Prometheus series exported for E6 and a sample of /metrics
- `pytest_output.txt`: test run (22 passed)
- `run_all.log`, `e7.log`, `e8.log`, `soak.log`: console output of the experiment runs
- `e5b_aborted.log`: an aborted saturation run, excluded because the load generator was CPU-bound (see report §9.2 E5)
- `raw/<exp>/<profile>/rep<i>/`: raw data per run (requests.csv, summary.json, integrity.json,
  alerts.json, counters.json, plan.json, logs/ with JSON service logs + supervisor events)

Note: `counters.json` holds Prometheus `increase()` values, which are extrapolated. The report uses exact counts from the logs (see `experiments/analyze.py::log_counts`).
