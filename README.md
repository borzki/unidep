# UniDep: a dependable university enrollment system

This is the Final Project for *Fault Tolerance and Dependable Computing*. It covers Option A, the university information system.

It is a small distributed system: four FastAPI microservices, an API gateway and PostgreSQL. It is instrumented with Prometheus and Grafana. Each fault-tolerance mechanism can be switched on or off, so the same code can be measured in a **baseline** profile and an **improved** profile under controlled fault injection.

## Layout

```
common/            shared library: config flags, resilience (timeouts, retries, circuit breaker),
                   security (JWT, RBAC, audit), observability (JSON logs, metrics), chaos injection
services/
  gateway/         routing, round-robin LB, health-check ejection, failover, rate limiting
  auth/            login -> JWT, brute-force lockout, audit log
  course/          catalogue + seat reservations (idempotent per enrollment)
  enrollment/      critical multi-step workflow, idempotency, checkpoint recovery, outbox
  notification/    user notifications (deduplicated)
db/                schema (init.sql) + reset/seed tool (reset.py)
deploy/
  local/           process supervisor (replicas + restart policy) used for the experiments
  prometheus/      scrape configs + alert rules (alerts.yml)
  grafana/         provisioned datasource + dashboard
  docker/          env files for docker-compose profiles
experiments/       harness, E1-E8 + soak (R), analysis
tests/             unit tests (mechanisms) + acceptance tests (running stack)
results/           raw data (requests.csv, summaries, integrity, alerts, logs) + tables + charts
docs/REPORT.md     final technical report
```

## Profiles

| Mechanism | baseline | improved | flag |
|---|---|---|---|
| Replicas of course and enrollment | 1 | 2 | topology |
| Restart policy | none | restart on exit (1 s) | supervisor / `RESTART_POLICY` |
| Gateway health checks + ejection | off | on (1 s) | `GW_HEALTH_CHECKS` |
| Gateway failover for retry-safe requests | off | on | `GW_FAILOVER` |
| Call timeout | 30 s | 1 s | `CALL_TIMEOUT_S` |
| Bounded retries, exp. backoff + jitter | off | 3 retries, 0.1-1 s | `FT_RETRIES` |
| Circuit breaker | off | 5 failures / 5 s | `FT_CIRCUIT_BREAKER` |
| Idempotency-Key + unique constraints | off | on | `FT_IDEMPOTENCY` |
| Checkpoint recovery of PENDING workflows | off | on (every 2 s) | `FT_RECOVERY` |
| Notification outbox (graceful degradation) | off (sync call) | on | `FT_OUTBOX` |
| DB transient-error retry | off | on | `FT_DB_RETRY` |
| Per-user rate limiting | off | 20 req/s, burst 40 | `FT_RATE_LIMIT` |
| Database backups (pg_dump) | none | yes | experiment E8 |

The security controls (JWT authentication, RBAC, object-level authorisation, input validation, body-size limit, login lockout, internal-token protection and audit log) are on in both profiles. Only rate limiting differs.

## Quick start: local runtime (this is how the reported results were produced)

You need Python 3.11+, PostgreSQL 16 and the Prometheus 2.54 binary.

```bash
pip install -r requirements.txt
# PostgreSQL listening on 127.0.0.1:5432, user postgres (trust or set DATABASE_URL)
python -m db.reset --mode improved
python deploy/local/supervisor.py start --mode improved      # gateway on :8000
prometheus --config.file=deploy/prometheus/prometheus-local.yml   # run from deploy/prometheus/
pytest -q tests/                                              # unit + acceptance tests
```

Try the workflow:

```bash
T=$(curl -s -XPOST localhost:8000/auth/login -H 'content-type: application/json' \
     -d '{"username":"student0001","password":"student-pass"}' | jq -r .access_token)
curl -s -XPOST localhost:8000/enrollments -H "authorization: Bearer $T" \
     -H 'content-type: application/json' -H 'idempotency-key: demo-1' -d '{"course_id":1}'
```

The seeded accounts are synthetic test users: `student0001`..`student0500` with password `student-pass`, and `admin` with password `admin-pass`. No real personal data is used.

## Container deployment

```bash
# improved (2 replicas of course + enrollment, restart: unless-stopped)
docker compose --env-file deploy/docker/improved.env --profile replicas up -d --build
# baseline
docker compose --env-file deploy/docker/baseline.env up -d --build
```

**Web console:** http://localhost:8000. Sign in with a test student, enroll, resend with the same key, and watch every replica's readiness and breaker state. The fault-injection buttons (Crash now, Crash mid-transaction, Slow +3 s, Fail 30%/100%, Heal) relay to the token-protected `/chaos` endpoints.

Grafana runs at http://localhost:3000 (dashboard *UniDep - Dependability*) and Prometheus at http://localhost:9090. The compose file was validated with `docker compose config`. The measured experiments were run on the local runtime, because the lab environment could not pull container images. Fault equivalents in Docker:

| Local harness | Docker equivalent |
|---|---|
| `supervisor.py kill enrollment 0` | crash from inside: `POST :8301/chaos {"crash_point":"after_reserve"}` then send enrollments, or `docker exec <project>-enrollment-1-1 kill 1`. Note: `docker kill` counts as a *manual* stop, so Docker deliberately does **not** apply the restart policy after it; failover still works, but no auto-restart. |
| `pg_ctl stop -m immediate` | `docker stop <project>-postgres-1` (then `start`) |
| `POST :8201/chaos` | same endpoint on the published port, header `x-chaos-token` |

## Reproducing the experiments

PostgreSQL and Prometheus must be running. The harness starts and stops the services itself.

```bash
cd experiments
python run.py all --reps 3          # E1-E6 baseline vs improved   (~45 min)
python security_tests.py            # E7 misuse / security cases
python backup_restore.py --reps 3   # E8 backup-restore + compensation
python soak.py --duration 300       # R  reliability soak (MTTF/MTTR/availability)
python analyze.py                   # results/summary.{csv,md}, reliability.json, charts/
```

Every run writes `results/raw/<exp>/<mode>/rep<i>/`, which contains:
- `plan.json`: the hypothesis, workload, fault and success criterion, fixed before the run
- `requests.csv`: one row per request
- `summary.json`
- `integrity.json`: data-consistency checks, taken right after the load and again after 15 s
- `alerts.json`: Prometheus alerts with their first firing time
- `counters.json`: retries, circuit-breaker opens, failovers and recovery actions
- `logs/`: JSON logs of every service instance, correlated by `request_id`

## Safety and integrity notes

- The `/chaos` endpoints are disabled unless `CHAOS_ENABLED=1`, and they require a token. Use them only in an isolated environment.
- Every secret in this repository is a placeholder for local testing. Set real values through the environment.

## Notes on the Docker deployment

- `db-init` runs with `--if-empty`, so rebuilding or redeploying keeps existing data. To start from a clean database, or to switch between the baseline and improved profiles (they use different integrity constraints), run `docker compose down -v` first.
- `docker kill` counts as a manual stop, so Docker does not apply the restart policy after it. Use the console's "Crash now" button or `POST /chaos {"crash_point":"now"}` to simulate a real crash.
