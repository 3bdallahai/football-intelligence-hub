# ============================================================
# ingestion/snowflake_connector.py
# ============================================================
# PURPOSE:
#   Provides a single, reusable function for getting a
#   Snowflake connection. All scripts import from here —
#   credentials are never hardcoded anywhere else.
#
# WHY IT EXISTS:
#   Centralises the connection logic. If your credentials
#   change, you only update the .env file, not every script.
#
# HOW IT CONNECTS TO THE PIPELINE:
#   initial_load.py calls get_snowflake_connection() to get
#   a live connection, then uses write_pandas() to load data.
# ============================================================

import os
import logging
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

# Load .env file (works locally; inside Docker the env vars come from docker-compose.yml)
load_dotenv()

logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """
    Create and return an active Snowflake connection using environment variables.

    Returns:
        snowflake.connector.SnowflakeConnection

    Raises:
        ValueError: if any required environment variable is missing
        snowflake.connector.errors.Error: if connection fails
    """

    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_ROLE',
    ]

    # Check all required environment variables are present
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise ValueError(
            f"Missing required environment variables: {missing}\n"
            f"Did you create a .env file from .env.example?"
        )

    account   = os.getenv('SNOWFLAKE_ACCOUNT')
    user      = os.getenv('SNOWFLAKE_USER')
    password  = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database  = os.getenv('SNOWFLAKE_DATABASE')
    role      = os.getenv('SNOWFLAKE_ROLE')

    logger.info(f"[SNOWFLAKE] Connecting → account={account}, user={user}, db={database}")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema='RAW',             # Default schema for Day 1
        role=role,
        login_timeout=30,
        network_timeout=30,
    )

    logger.info("[SNOWFLAKE] Connection established ✓")
    return conn


def test_connection():
    """
    Quick connectivity test. Run this file directly to verify your credentials.
    Returns True on success, False on failure.
    """
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor(DictCursor)
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        row = cursor.fetchone()
        print(f"\n✅ Snowflake connection successful!")
        print(f"   User:     {row['CURRENT_USER()']}")
        print(f"   Database: {row['CURRENT_DATABASE()']}")
        print(f"   Schema:   {row['CURRENT_SCHEMA()']}")
        conn.close()
        return True
    except Exception as e:
        print(f"\n❌ Snowflake connection failed: {e}")
        print("\nCheck your .env file — especially SNOWFLAKE_ACCOUNT format.")
        print("Account format example:  abc12345.us-east-1")
        return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s — %(message)s')
    test_connection()
