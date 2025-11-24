import os
import sys
from pathlib import Path
import pandas as pd

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.db_connections import get_postgres_connection
from utils.logger import setup_logger

# Paths
DATA_RAW_DIR = Path(os.getenv("DATA_RAW_PATH", PROJECT_ROOT / "data" / "raw"))
SQL_DIR = PROJECT_ROOT / "sql"
LOGS_DIR = PROJECT_ROOT / "logs"

# SQL file for creating raw table
CREATE_RAW_TABLE_SQL = SQL_DIR / "create_raw_table.sql"

# CSV file
CSV_FILE = DATA_RAW_DIR / "base-desapariciones-dataton-2025.csv"

# Table name
TABLE_NAME = "raw.desapariciones_niños_raw"

# Columns in order, matching SQL & CSV
COLUMNS = (
    "sexo, edad, grupo_etario, municipio, region, colonia_localidad, "
    "migrante, fecha_desaparicion, dia_semana, horario, estatus, "
    "fecha_localizacion, dias_sin_localizar, rango_desaparicion, "
    "reincidencia, numero_reincidencia, desaparicion_multiple, "
    "persona_con_quien_desaparecio, hipotesis, fuente, sistematizo"
)

def execute_sql_file(cursor, sql_file: Path):
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    with open(sql_file, "r") as f:
        sql = f.read()
    cursor.execute(sql)


def main():
    logger = setup_logger(__name__, log_file=LOGS_DIR / "load_desapariciones.log")

    # Ensure directories exist
    LOGS_DIR.mkdir(exist_ok=True)
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Connect to DB
        conn = get_postgres_connection(
            db_name=os.getenv("PROJECT_POSTGRES_DB", "desapariciones")
        )
        cursor = conn.cursor()

        # Create raw table
        logger.info(f"Ensuring table {TABLE_NAME} exists using {CREATE_RAW_TABLE_SQL}")
        execute_sql_file(cursor, CREATE_RAW_TABLE_SQL)
        conn.commit()

        if not CSV_FILE.exists():
            raise FileNotFoundError(f"CSV not found: {CSV_FILE}")

        # Skip load if table already has data
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        row_count = cursor.fetchone()[0]
        if row_count > 0:
            logger.info(f"Table already contains {row_count} rows, skipping load.")
            return

        # Load CSV using Pandas to handle potential formatting issues (extra cols)
        logger.info(f"Reading CSV {CSV_FILE} with pandas...")
        df = pd.read_csv(CSV_FILE, encoding="utf-8")

        # Handle extra columns (e.g. trailing delimiters)
        expected_col_count = 21
        if len(df.columns) > expected_col_count:
            logger.warning(
                f"CSV has {len(df.columns)} columns, expected {expected_col_count}. "
                "Dropping extra columns."
            )
            df = df.iloc[:, :expected_col_count]

        # Use a buffer to load data via COPY
        from io import StringIO
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        logger.info(f"Loading {len(df)} rows into {TABLE_NAME}...")
        cursor.copy_expert(
            f"COPY {TABLE_NAME} ({COLUMNS}) FROM STDIN WITH CSV",
            buffer
        )

        conn.commit()
        logger.info("CSV loaded successfully.")

    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        if 'conn' in locals():
            conn.rollback()
        sys.exit(1)

    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
