# ============================================================
# ingestion/initial_load.py (STABLE + OPTIMIZED VERSION)
# ============================================================

import os
import sys
import uuid
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from snowflake.connector.pandas_tools import write_pandas

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.player_type_detector import detect_player_type
from ingestion.snowflake_connector import get_snowflake_connection


# ────────────────────────────────────────────────────────────
# ENV
# ────────────────────────────────────────────────────────────
load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "./data/raw")

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FOOTBALL_ANALYTICS")
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_TABLE = "BRONZE_PLAYERS"

BATCH_SIZE = 100_000
MAX_WORKERS = min(3, os.cpu_count() or 2)


# ────────────────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# STEP 1 — SCAN FILES
# ============================================================
def scan_csv_files(root_path: str):

    root = Path(root_path)
    files = []

    if not root.exists():
        logger.error(f"Path not found: {root}")
        return []

    for path in root.rglob("*.csv"):

        parts = path.relative_to(root).parts

        league = parts[0] if len(parts) >= 3 else "UNKNOWN_LEAGUE"
        team = parts[1] if len(parts) >= 3 else "UNKNOWN_TEAM"

        files.append({
            "file_path": str(path),
            "file_name": path.name,
            "league": league,
            "team": team
        })

    logger.info(f"[SCAN] Found {len(files)} files")
    return files


# ============================================================
# STEP 2 — CLEAN
# ============================================================
def clean_dataframe(df: pd.DataFrame):

    df.columns = df.columns.str.strip()

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    return df


# ============================================================
# STEP 3 — READ + PREPARE (STABLE VERSION)
# ============================================================
def read_and_prepare_csv(file_info: dict, batch_id: str):

    try:
        df = pd.read_csv(
            file_info["file_path"],
            encoding="utf-8",
            encoding_errors="replace",
            low_memory=False
        )

        if df.empty:
            return None

        df = clean_dataframe(df)

        player_type = detect_player_type(df, file_info["file_name"])

        # ── normalize column names ───────────────────────
        df.columns = (
            df.columns
            .str.upper()
            .str.replace(r"[^A-Z0-9_]", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )

        # ── metadata ────────────────────────────────────
        df["_SOURCE_FILE"] = file_info["file_path"]
        df["_LEAGUE"] = file_info["league"]
        df["_TEAM"] = file_info["team"]
        df["_PLAYER_TYPE"] = player_type
        df["_BATCH_ID"] = batch_id
        df["_INGEST_TS"] = datetime.now(timezone.utc)

        # ── IMPORTANT: restore original safe behavior ───
        # (prevents pyarrow type crashes)
        source_cols = [c for c in df.columns if not c.startswith("_")]

        df[source_cols] = (
            df[source_cols]
            .astype(str)
            .replace("nan", None)
        )

        return df

    except Exception as e:
        logger.error(f"Failed {file_info['file_name']}: {e}")
        return None


# ============================================================
# STEP 4 — SCHEMA SYNC (ONCE ONLY)
# ============================================================
def sync_table_schema_from_columns(all_columns, conn):

    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
              AND TABLE_NAME = '{SNOWFLAKE_TABLE}'
        """)

        existing = {r[0].upper() for r in cursor.fetchall()}
        incoming = {c.upper() for c in all_columns}

        missing = incoming - existing

        if not missing:
            logger.info("[SCHEMA] No changes needed")
            return

        logger.info(f"[SCHEMA] Adding {len(missing)} columns")

        for col in missing:
            cursor.execute(f"""
                ALTER TABLE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
                ADD COLUMN IF NOT EXISTS "{col}" VARCHAR
            """)

    finally:
        cursor.close()


# ============================================================
# STEP 5 — LOAD SINGLE GROUP
# ============================================================
def load_single_group(group_id, dfs):

    conn = get_snowflake_connection()

    try:
        df = pd.concat(dfs, ignore_index=True, sort=False)

        success, chunks, rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=SNOWFLAKE_TABLE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
            overwrite=False,
            auto_create_table=False,
            quote_identifiers=False,
            chunk_size=BATCH_SIZE
        )

        if success:
            logger.info(f"[LOAD] Group {group_id} → {rows:,} rows")
            return rows

        logger.error(f"[LOAD] Group {group_id} failed")
        return 0

    finally:
        conn.close()


# ============================================================
# STEP 6 — PARALLEL LOAD
# ============================================================
def bulk_load_parallel(schema_groups):

    total = 0

    logger.info(f"[PARALLEL] Workers = {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:

        futures = []

        for i, (_, dfs) in enumerate(schema_groups.items(), 1):
            futures.append(ex.submit(load_single_group, i, dfs))

        for f in as_completed(futures):
            total += f.result() or 0

    return total


# ============================================================
# MAIN PIPELINE
# ============================================================
def run_initial_load():

    batch_id = f"{datetime.now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"

    logger.info("=" * 60)
    logger.info(f"INITIAL LOAD STARTED | {batch_id}")
    logger.info("=" * 60)

    files = scan_csv_files(RAW_DATA_PATH)

    if not files:
        logger.error("No files found")
        return

    conn = get_snowflake_connection()

    schema_groups = defaultdict(list)
    all_columns = set()

    stats = {
        "processed": 0,
        "skipped": 0,
        "loaded": 0
    }

    # ── READ PHASE ─────────────────────────────
    for f in files:

        df = read_and_prepare_csv(f, batch_id)

        if df is None:
            stats["skipped"] += 1
            continue

        schema_groups[tuple(sorted(df.columns))].append(df)
        all_columns.update(df.columns)

        stats["processed"] += 1

    # ── SCHEMA SYNC ONCE ───────────────────────
    sync_table_schema_from_columns(all_columns, conn)
    conn.close()

    # ── LOAD PHASE ─────────────────────────────
    stats["loaded"] = bulk_load_parallel(schema_groups)

    # ── SUMMARY ────────────────────────────────
    logger.info("=" * 60)
    logger.info("DONE")
    logger.info(f"Processed: {stats['processed']}")
    logger.info(f"Skipped: {stats['skipped']}")
    logger.info(f"Rows loaded: {stats['loaded']:,}")
    logger.info("=" * 60)


# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_initial_load()