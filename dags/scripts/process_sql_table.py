import os
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.db_connections import get_postgres_connection
from utils.logger import setup_logger

# Paths
SQL_DIR = PROJECT_ROOT / "sql"
LOGS_DIR = PROJECT_ROOT / "logs"

CREATE_PROCESSED_TABLE_SQL = SQL_DIR / "create_processed_table.sql"
TRANSFORM_SQL = SQL_DIR / "transform_raw_to_processed.sql"

PROCESSED_TABLE = "processed.desapariciones_niños_processed"


def execute_sql_file(cursor, sql_file: Path):
    """Execute SQL file."""
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    with open(sql_file, "r") as f:
        sql = f.read()
        cursor.execute(sql)


def main():
    logger = setup_logger(__name__, LOGS_DIR / "transform_raw_to_processed.log")

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        # Create processed table
        logger.info(f"Ensuring processed table exists: {PROCESSED_TABLE}")
        execute_sql_file(cursor, CREATE_PROCESSED_TABLE_SQL)
        conn.commit()

        # Check if processed table already has data (idempotent)
        cursor.execute(f"SELECT COUNT(*) FROM {PROCESSED_TABLE}")
        if cursor.fetchone()[0] > 0:
            logger.info(f"Table {PROCESSED_TABLE} already populated. Skipping transformation.")
            return

        # Execute transformation SQL
        logger.info(f"Executing transformation SQL from {TRANSFORM_SQL}")
        execute_sql_file(cursor, TRANSFORM_SQL)
        conn.commit()

        logger.info("Transformation completed successfully.")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        if "conn" in locals():
            conn.rollback()
        sys.exit(1)

    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
