# utils/db_connection.py
import os
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv

load_dotenv()

def get_postgres_connection(db_name: Optional[str] = None) -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=db_name or os.getenv('PROJECT_POSTGRES_DB', 'desapariciones'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'secret123')
        )
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Failed to connect to PostgreSQL: {e}")