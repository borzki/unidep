"""PostgreSQL access with startup retry, transient-error retry and readiness ping."""
import asyncio
import logging

import asyncpg

from . import config
from .obs import get_logger, log
from .resilience import backoff_delay

logger = get_logger("db")

TRANSIENT = (
    asyncpg.exceptions.ConnectionDoesNotExistError,
    asyncpg.exceptions.CannotConnectNowError,
    asyncpg.exceptions.AdminShutdownError,
    asyncpg.exceptions.InterfaceError,
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
)


class Database:
    def __init__(self, schema: str):
        self.schema = schema
        self.pool: asyncpg.Pool | None = None

    async def connect(self, attempts: int = 30):
        for i in range(attempts):
            try:
                self.pool = await asyncpg.create_pool(
                    config.DATABASE_URL, min_size=1, max_size=10, timeout=2.0,
                    command_timeout=5.0, server_settings={"search_path": self.schema},
                )
                return
            except Exception as exc:  # noqa: BLE001
                log(logger, logging.WARNING, "db_connect_failed", attempt=i + 1, error=str(exc))
                await asyncio.sleep(min(2.0, 0.2 * (i + 1)))
        raise RuntimeError("database unavailable at startup")

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def run(self, fn, *, retry: bool | None = None):
        """Run `fn(conn)` inside a transaction. Transient connection errors are
        retried (bounded, backoff) when FT_DB_RETRY is on."""
        retry = config.FT_DB_RETRY if retry is None else retry
        attempts = 4 if retry else 1
        for attempt in range(attempts):
            try:
                async with self.pool.acquire(timeout=2.0) as conn:
                    async with conn.transaction():
                        return await fn(conn)
            except TRANSIENT as exc:
                if attempt == attempts - 1:
                    raise
                log(logger, logging.WARNING, "db_retry", attempt=attempt + 1, error=type(exc).__name__)
                await asyncio.sleep(backoff_delay(attempt, base=0.2, cap=1.0))

    async def ping(self) -> bool:
        try:
            async with self.pool.acquire(timeout=0.5) as conn:
                await asyncio.wait_for(conn.fetchval("SELECT 1"), 0.5)
            return True
        except Exception:  # noqa: BLE001
            return False
