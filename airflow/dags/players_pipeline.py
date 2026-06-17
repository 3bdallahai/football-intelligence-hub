"""
pipeline_dag.py
---------------
Orchestrates:
  1. Python ingestion  →  loads data into Snowflake bronze layer
  2. dbt run silver    →  bronze → silver
  3. dbt run gold      →  silver → gold
  4. dbt run similarity→  gold → similarity mart
  5. dbt run valuation →  gold → valuation mart
"""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Ingestion callable
# ---------------------------------------------------------------------------
INGESTION_SCRIPT = Path("/opt/airflow/plugins/ingestion/initial_load.py")


def run_ingestion(**context):
    """Load ingestion script and call its run() function."""
    if not INGESTION_SCRIPT.exists():
        raise FileNotFoundError(f"Ingestion script not found: {INGESTION_SCRIPT}")

    spec = importlib.util.spec_from_file_location("initial_load", INGESTION_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["initial_load"] = module
    spec.loader.exec_module(module)
    module.run_initial_load()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR  = "/opt/airflow/plugins/football_analytics"
DBT_PROFILES_DIR = "/opt/airflow/plugins/football_analytics"
DBT_CMD          = f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod"

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="new_season_data_pipeline",
    description="Ingest → silver → gold → similarity → valuation",
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=["snowflake", "dbt", "ingestion"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Python ingestion → bronze
    # ------------------------------------------------------------------
    ingest = PythonOperator(
        task_id="python_ingestion",
        python_callable=run_ingestion,
        doc_md="Loads raw CSV data into Snowflake bronze layer.",
    )

    # ------------------------------------------------------------------
    # Task 2: bronze → silver
    # ------------------------------------------------------------------
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=f"{DBT_CMD} --select silver",
        doc_md="Cleans and types raw bronze data into the silver layer.",
    )

    # ------------------------------------------------------------------
    # Task 3: silver → gold
    # ------------------------------------------------------------------
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=f"{DBT_CMD} --select gold",
        doc_md="Applies business logic to produce the gold layer.",
    )

    # ------------------------------------------------------------------
    # Task 4: gold → similarity mart
    # ------------------------------------------------------------------
    dbt_similarity = BashOperator(
        task_id="dbt_similarity",
        bash_command=f"{DBT_CMD} --select similarity",
        doc_md="Builds the similarity mart from the gold layer.",
    )

    # ------------------------------------------------------------------
    # Task 5: gold → valuation mart
    # ------------------------------------------------------------------
    dbt_valuation = BashOperator(
        task_id="dbt_valuation",
        bash_command=f"{DBT_CMD} --select valuation",
        doc_md="Builds the valuation mart from the gold layer.",
    )

    # ------------------------------------------------------------------
    # Dependencies
    #
    #   ingest → silver → gold → similarity
    #                          → valuation
    #
    # similarity and valuation both depend on gold but run in parallel.
    # ------------------------------------------------------------------
    ingest >> dbt_silver >> dbt_gold >> [dbt_similarity, dbt_valuation]