"""Background jobs, run by an arq worker on a cron schedule.

Start it with: arq spoolman.worker.WorkerSettings
"""

import logging
from collections.abc import Awaitable, Callable
from typing import ClassVar

from arq import cron
from arq.connections import RedisSettings
from sqlalchemy import text

from spoolman import env, externaldb, update_check
from spoolman.database import database

logger = logging.getLogger(__name__)


async def _run(job: Callable[[], Awaitable[object]]) -> None:
    database.setup_db(database.get_connection_url())
    await job()


async def collect_metrics(_ctx: dict) -> None:
    """Refresh the Prometheus metrics."""
    if env.is_metrics_enabled():
        await _run(database._metrics)  # noqa: SLF001


async def backup_database(_ctx: dict) -> None:
    """Back up the SQLite database."""
    if env.is_automatic_backup_enabled():
        await _run(database._backup_task)  # noqa: SLF001


async def sync_external_db(_ctx: dict) -> None:
    """Sync filaments and materials from the external DB."""
    if externaldb.get_external_db_url().strip():
        await _run(externaldb._sync)  # noqa: SLF001


async def check_for_updates(_ctx: dict) -> None:
    """Check GitHub for a newer Spoolman release."""
    if env.is_update_check_enabled():
        await _run(update_check._check)  # noqa: SLF001


async def _vacuum() -> None:
    async for session in database.get_db_session():
        if session.bind.dialect.name == "sqlite":
            await session.execute(text("VACUUM"))
        logger.info("Database vacuum done.")


async def vacuum_database(_ctx: dict) -> None:
    """Compact the SQLite database."""
    await _run(_vacuum)


class WorkerSettings:
    """arq worker settings: the cron jobs and the Redis connection."""

    redis_settings = RedisSettings.from_dsn(env.get_redis_url())
    functions: ClassVar[list] = []
    cron_jobs: ClassVar[list] = [
        cron(collect_metrics, second=0),
        cron(backup_database, hour=0, minute=0),
        cron(sync_external_db, hour={0, 6, 12, 18}, minute=0),
        cron(check_for_updates, hour={0, 12}, minute=30),
        cron(vacuum_database, weekday="sun", hour=3, minute=0),
    ]
