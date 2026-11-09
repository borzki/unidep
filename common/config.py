"""Central configuration.

RESILIENCE_MODE selects a profile ("baseline" or "improved"). Every mechanism
can also be switched individually with its own environment variable so that a
single mechanism can be isolated in an experiment (e.g. FT_RETRIES=0).
"""
import os

MODE = os.getenv("RESILIENCE_MODE", "improved").lower()
IMPROVED = MODE == "improved"


def _flag(name: str, improved_default: bool) -> bool:
    raw = os.getenv(name)
    if raw is not None:
        return raw.strip().lower() in ("1", "true", "yes", "on")
    return improved_default if IMPROVED else False


SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown")
INSTANCE_ID = os.getenv("INSTANCE_ID", "0")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/unidep")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-only-not-a-secret-change-me-32b+")  # never a real secret
JWT_TTL_S = int(os.getenv("JWT_TTL_S", "3600"))
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "dev-internal-token")
CHAOS_TOKEN = os.getenv("CHAOS_TOKEN", "dev-chaos-token")

# --- software fault-tolerance mechanisms -----------------------------------
FT_RETRIES = _flag("FT_RETRIES", True)            # bounded retries + exp. backoff + jitter
FT_CIRCUIT_BREAKER = _flag("FT_CIRCUIT_BREAKER", True)
FT_IDEMPOTENCY = _flag("FT_IDEMPOTENCY", True)    # Idempotency-Key + duplicate detection
FT_RECOVERY = _flag("FT_RECOVERY", True)          # checkpoint + background recovery of PENDING
FT_OUTBOX = _flag("FT_OUTBOX", True)              # graceful degradation of notifications
FT_DB_RETRY = _flag("FT_DB_RETRY", True)          # retry transient DB connection errors
FT_RATE_LIMIT = _flag("FT_RATE_LIMIT", True)

# Timeouts: improved uses tight, explicit budgets; baseline uses a long,
# effectively unbounded client timeout (typical framework default behaviour).
CALL_TIMEOUT_S = float(os.getenv("CALL_TIMEOUT_S", "1.0" if IMPROVED else "30.0"))
RETRY_MAX = int(os.getenv("RETRY_MAX", "3"))
RETRY_BASE_S = float(os.getenv("RETRY_BASE_S", "0.1"))
RETRY_CAP_S = float(os.getenv("RETRY_CAP_S", "1.0"))
CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
CB_RESET_S = float(os.getenv("CB_RESET_S", "5.0"))

RECOVERY_INTERVAL_S = float(os.getenv("RECOVERY_INTERVAL_S", "2.0"))
RECOVERY_MIN_AGE_S = float(os.getenv("RECOVERY_MIN_AGE_S", "5.0"))
OUTBOX_INTERVAL_S = float(os.getenv("OUTBOX_INTERVAL_S", "1.0"))

COURSE_URLS = [u for u in os.getenv("COURSE_URLS", "http://127.0.0.1:8201").split(",") if u]
NOTIFICATION_URLS = [u for u in os.getenv("NOTIFICATION_URLS", "http://127.0.0.1:8401").split(",") if u]

# Gateway
UPSTREAMS = os.getenv(
    "UPSTREAMS",
    "auth=http://127.0.0.1:8101;course=http://127.0.0.1:8201;"
    "enrollment=http://127.0.0.1:8301;notification=http://127.0.0.1:8401",
)
GW_HEALTH_CHECKS = _flag("GW_HEALTH_CHECKS", True)
GW_FAILOVER = _flag("GW_FAILOVER", True)
GW_HEALTH_INTERVAL_S = float(os.getenv("GW_HEALTH_INTERVAL_S", "1.0"))
GW_UPSTREAM_TIMEOUT_S = float(os.getenv("GW_UPSTREAM_TIMEOUT_S", "5.0" if IMPROVED else "30.0"))
RATE_LIMIT_RPS = float(os.getenv("RATE_LIMIT_RPS", "20"))
RATE_LIMIT_BURST = float(os.getenv("RATE_LIMIT_BURST", "40"))
LOGIN_MAX_FAILURES = int(os.getenv("LOGIN_MAX_FAILURES", "5"))
LOGIN_WINDOW_S = int(os.getenv("LOGIN_WINDOW_S", "60"))
MAX_BODY_BYTES = int(os.getenv("MAX_BODY_BYTES", "16384"))


def describe() -> dict:
    return {
        "mode": MODE,
        "retries": FT_RETRIES,
        "circuit_breaker": FT_CIRCUIT_BREAKER,
        "idempotency": FT_IDEMPOTENCY,
        "recovery": FT_RECOVERY,
        "outbox": FT_OUTBOX,
        "db_retry": FT_DB_RETRY,
        "rate_limit": FT_RATE_LIMIT,
        "call_timeout_s": CALL_TIMEOUT_S,
        "gw_health_checks": GW_HEALTH_CHECKS,
        "gw_failover": GW_FAILOVER,
    }
