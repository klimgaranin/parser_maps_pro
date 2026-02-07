import os

from core.db_sqlite import SQLiteDB
from core.db_pg import PostgresDB


def make_db():
    kind = os.getenv("DB_KIND", "sqlite").strip().lower()

    if kind == "postgres":
        dsn = os.getenv("DB_DSN", "").strip()
        if not dsn:
            raise SystemExit("DB_KIND=postgres but DB_DSN is empty")
        return PostgresDB(dsn)

    path = os.getenv("DB_PATH", "./output/progress.sqlite").strip()
    return SQLiteDB(path)
