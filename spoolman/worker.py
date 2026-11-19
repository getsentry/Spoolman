"""Background jobs, run by Celery beat and a Celery worker.

Start both with: celery -A spoolman.worker worker --beat --loglevel=info
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from celery import Celery
from celery.schedules import crontab
from sqlalchemy import text

from spoolman import env, externaldb, update_check
from spoolman.database import database

logger = logging.getLogger(__name__)

app = Celery("spoolman", broker=env.get_celery_broker_url())
app.conf.timezone = "UTC"
app.conf.beat_schedule = {
    "collect-metrics": {
        "task": "spoolman.worker.collect_metrics",
        "schedule": crontab(),
    },
    "backup-database": {
        "task": "spoolman.worker.backup_database",
        "schedule": crontab(hour=0, minute=0),
    },
    "sync-external-db": {
        "task": "spoolman.worker.sync_external_db",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "check-for-updates": {
        "task": "spoolman.worker.check_for_updates",
        "schedule": crontab(minute=30, hour="*/12"),
    },
    "vacuum-database": {
        "task": "spoolman.worker.vacuum_database",
        "schedule": crontab(hour=3, minute=0, day_of_week="sun"),
    },
}


def _run(job: Callable[[], Awaitable[object]]) -> None:
    database.setup_db(database.get_connection_url())
    asyncio.run(job())


@app.task
def collect_metrics() -> None:
    """Refresh the Prometheus metrics."""
    if env.is_metrics_enabled():
        _run(database._metrics)  # noqa: SLF001


@app.task
def backup_database() -> None:
    """Back up the SQLite database."""
    if env.is_automatic_backup_enabled():
        _run(database._backup_task)  # noqa: SLF001


@app.task
def sync_external_db() -> None:
    """Sync filaments and materials from the external DB."""
    if externaldb.get_external_db_url().strip():
        _run(externaldb._sync)  # noqa: SLF001


@app.task
def check_for_updates() -> None:
    """Check GitHub for a newer Spoolman release."""
    if env.is_update_check_enabled():
        _run(update_check._check)  # noqa: SLF001


async def _vacuum() -> None:
    async for session in database.get_db_session():
        if session.bind.dialect.name == "sqlite":
            await session.execute(text("VACUUM"))
        logger.info("Database vacuum done.")


@app.task
def vacuum_database() -> None:
    """Compact the SQLite database."""
    _run(_vacuum)
