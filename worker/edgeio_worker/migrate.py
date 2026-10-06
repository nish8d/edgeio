"""Apply db/migrations/*.sql in name order, each once, each in its own transaction."""

import logging
from pathlib import Path

import psycopg

from .config import WorkerSettings
from .logs import configure_logging

log = logging.getLogger(__name__)

MIGRATION_LOCK_ID = 7_262_001  # arbitrary; serializes concurrent migrators


NO_TRANSACTION_MARKER = "-- migrate: no-transaction"


def statements(script: str) -> list[str]:
    """Split a simple migration script (no semicolons inside literals) into statements."""
    code = "\n".join(line for line in script.splitlines() if not line.lstrip().startswith("--"))
    return [part.strip() for part in code.split(";") if part.strip()]


def apply_migrations(database_url: str, migrations_dir: Path) -> list[str]:
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no migrations found in {migrations_dir}")
    applied: list[str] = []
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version text PRIMARY KEY,"
                " applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for path in files:
                if path.name in done:
                    continue
                script = path.read_text()
                if script.startswith(NO_TRANSACTION_MARKER):
                    # e.g. refresh_continuous_aggregate(), which refuses to run in a transaction
                    for statement in statements(script):
                        conn.execute(statement)
                    conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,)
                    )
                    log.info("applied migration", extra={"migration": path.name})
                    applied.append(path.name)
                    continue
                with conn.transaction():
                    conn.execute(script)
                    conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,)
                    )
                log.info("applied migration", extra={"migration": path.name})
                applied.append(path.name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
    return applied


def main() -> None:
    settings = WorkerSettings()
    configure_logging(settings.log_level)
    applied = apply_migrations(settings.database_url, settings.migrations_dir)
    log.info("migrations complete", extra={"applied": applied})


if __name__ == "__main__":
    main()
