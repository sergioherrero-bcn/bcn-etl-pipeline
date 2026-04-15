"""
bcn_etl_dag.py
==============
End-to-end ETL DAG for the Barcelona Open Data pipeline.

Schedule: daily at 06:00 UTC (data for previous day is available by then).

Task groups
-----------
extract/       — Python extractors write raw data to PostgreSQL
transform/     — dbt run (staging → intermediate → marts)
test/          — dbt test on all models
"""

from __future__ import annotations

import logging
import subprocess
from datetime import date, datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── DAG defaults ──────────────────────────────────────────────────────────────

_DEFAULT_ARGS = {
    "owner":            "bcn-etl",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

_DBT_DIR = "/opt/airflow/dbt_project"
_DBT_PROFILES_DIR = "/opt/airflow/dbt_project"


# ── Python callables ─────────────────────────────────────────────────────────

def _extract_bicing_station_info() -> None:
    from src.extract.open_data_bcn import BicingExtractor
    rows = BicingExtractor().load_station_info()
    log.info("Bicing station info loaded: %d rows", rows)


def _extract_bicing_status(year: int, month: int) -> None:
    from src.extract.open_data_bcn import BicingExtractor
    rows = BicingExtractor().load_monthly_status(year, month)
    log.info("Bicing status loaded: %d rows (%d-%02d)", rows, year, month)


def _extract_meteo(year: int) -> None:
    from src.extract.open_data_bcn import MeteoExtractor
    rows = MeteoExtractor().load(year)
    log.info("Meteo loaded: %d rows (year=%d)", rows, year)


def _extract_noise(year: int, month: int) -> None:
    from src.extract.open_data_bcn import NoiseExtractor
    rows = NoiseExtractor().load(year, month)
    log.info("Noise loaded: %d rows (%d-%02d)", rows, year, month)


def _extract_geography() -> None:
    from src.extract.open_data_bcn import GeographyExtractor
    rows = GeographyExtractor().load()
    log.info("Administrative units loaded: %d rows", rows)


def _get_prev_month(execution_date: date) -> tuple[int, int]:
    """Return (year, month) for the month prior to execution_date."""
    first = execution_date.replace(day=1)
    prev = first - timedelta(days=1)
    return prev.year, prev.month


def _extract_bicing_prev_month(**context) -> None:
    year, month = _get_prev_month(context["data_interval_start"].date())
    _extract_bicing_status(year, month)


def _extract_noise_prev_month(**context) -> None:
    year, month = _get_prev_month(context["data_interval_start"].date())
    _extract_noise(year, month)


def _extract_meteo_current_year(**context) -> None:
    year = context["data_interval_start"].year
    _extract_meteo(year)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="bcn_etl_dag",
    description="Extract Barcelona Open Data → dbt transform → dbt test",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    max_active_runs=1,
    tags=["bcn", "etl", "dbt"],
) as dag:

    # ── Extract group ─────────────────────────────────────────────────────────
    with TaskGroup("extract") as extract_group:

        t_bicing_info = PythonOperator(
            task_id="bicing_station_info",
            python_callable=_extract_bicing_station_info,
        )

        t_bicing_status = PythonOperator(
            task_id="bicing_station_status",
            python_callable=_extract_bicing_prev_month,
        )

        t_meteo = PythonOperator(
            task_id="meteo_daily",
            python_callable=_extract_meteo_current_year,
        )

        t_noise = PythonOperator(
            task_id="noise_readings",
            python_callable=_extract_noise_prev_month,
        )

        t_geo = PythonOperator(
            task_id="administrative_units",
            python_callable=_extract_geography,
        )

    # ── dbt transform group ───────────────────────────────────────────────────
    with TaskGroup("transform") as transform_group:

        t_dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=(
                f"cd {_DBT_DIR} && "
                f"dbt deps --profiles-dir {_DBT_PROFILES_DIR}"
            ),
        )

        t_dbt_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=(
                f"cd {_DBT_DIR} && "
                f"dbt run --select staging --profiles-dir {_DBT_PROFILES_DIR}"
            ),
        )

        t_dbt_intermediate = BashOperator(
            task_id="dbt_run_intermediate",
            bash_command=(
                f"cd {_DBT_DIR} && "
                f"dbt run --select intermediate --profiles-dir {_DBT_PROFILES_DIR}"
            ),
        )

        t_dbt_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=(
                f"cd {_DBT_DIR} && "
                f"dbt run --select marts --profiles-dir {_DBT_PROFILES_DIR}"
            ),
        )

        t_dbt_deps >> t_dbt_staging >> t_dbt_intermediate >> t_dbt_marts

    # ── dbt test group ────────────────────────────────────────────────────────
    with TaskGroup("test") as test_group:

        t_dbt_test_staging = BashOperator(
            task_id="dbt_test_staging",
            bash_command=(
                f"cd {_DBT_DIR} && "
                f"dbt test --select staging --profiles-dir {_DBT_PROFILES_DIR}"
            ),
        )

        t_dbt_test_marts = BashOperator(
            task_id="dbt_test_marts",
            bash_command=(
                f"cd {_DBT_DIR} && "
                f"dbt test --select marts --profiles-dir {_DBT_PROFILES_DIR}"
            ),
        )

        t_dbt_test_staging >> t_dbt_test_marts

    # ── Dependencies ──────────────────────────────────────────────────────────
    extract_group >> transform_group >> test_group
