"""
Database helpers — SQLAlchemy engine factory and raw-schema loaders.

All writes to PostgreSQL in this project go through `load_dataframe`,
which handles table creation, type inference, and append-only inserts.
"""

from __future__ import annotations

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """Build a SQLAlchemy engine from environment variables.

    Expected env vars:
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
        POSTGRES_PASSWORD, POSTGRES_DB
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    dbname = os.environ["POSTGRES_DB"]
    dsn = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(dsn, pool_pre_ping=True)


def ensure_schema(engine: Engine, schema: str) -> None:
    """Create *schema* if it does not already exist."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    logger.debug("Schema ready: %s", schema)


def load_dataframe(
    df: pd.DataFrame,
    table: str,
    schema: str = "raw",
    engine: Engine | None = None,
    if_exists: str = "append",
    chunksize: int = 5_000,
) -> int:
    """Write *df* to PostgreSQL and return the number of rows inserted.

    Parameters
    ----------
    df        : DataFrame to persist.
    table     : Target table name (without schema prefix).
    schema    : Target schema (default: "raw").
    engine    : SQLAlchemy engine; built from env vars if not supplied.
    if_exists : "append" (default) or "replace".
    chunksize : Rows per INSERT batch.
    """
    if df.empty:
        logger.warning("load_dataframe called with empty DataFrame — skipping %s.%s", schema, table)
        return 0

    eng = engine or get_engine()
    ensure_schema(eng, schema)

    df.to_sql(
        name=table,
        con=eng,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method="multi",
    )
    rows = len(df)
    logger.info("Loaded %d rows → %s.%s", rows, schema, table)
    return rows
