import sys
from pathlib import Path
import os

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.db_connections import get_postgres_connection
from utils.logger import setup_logger

SQL_DIR = PROJECT_ROOT / "sql"
LOGS_DIR = PROJECT_ROOT / "logs"

CREATE_VIEWS_SQL = SQL_DIR / "create_trusted_views.sql"


def execute_sql_file(cursor, sql_file: Path):
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    cursor.execute(sql)


def main():
    logger = setup_logger(__name__, log_file=LOGS_DIR / "create_trusted_views.log")

    LOGS_DIR.mkdir(exist_ok=True)

    try:
        db_name = os.getenv("PROJECT_POSTGRES_DB", "desapariciones")
        conn = get_postgres_connection(db_name=db_name)
        cursor = conn.cursor()

        logger.info(f"Creating trusted schema views using file: {CREATE_VIEWS_SQL}")

        execute_sql_file(cursor, CREATE_VIEWS_SQL)
        conn.commit()

        logger.info("Successfully created all trusted views")

    except Exception as e:
        logger.error(f"Error creating trusted views: {e}")
        if "conn" in locals():
            conn.rollback()
        sys.exit(1)

    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
