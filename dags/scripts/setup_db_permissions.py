import os
import sys
import logging
import psycopg2
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_admin_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        database=os.environ.get('POSTGRES_DB', 'desapariciones'),
        user=os.environ.get('POSTGRES_USER', 'airflow'),
        password=os.environ.get('POSTGRES_PASSWORD', 'airflow')
    )

def setup_permissions():
    try:
        conn = get_admin_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("Checking/Creating 'data_analyst' role...")
        # Create role if not exists
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'data_analyst'")
        if not cursor.fetchone():
            cursor.execute("CREATE ROLE data_analyst LOGIN PASSWORD 'analyst123'")
            logger.info("Role 'data_analyst' created.")
        else:
            # Ensure password is correct (optional, but good for consistency)
            cursor.execute("ALTER ROLE data_analyst WITH PASSWORD 'analyst123'")
            logger.info("Role 'data_analyst' exists. Password updated.")

        logger.info("Ensuring schemas exist...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS processed")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS trusted")

        logger.info("Granting permissions...")
        # Grant CONNECT
        cursor.execute("GRANT CONNECT ON DATABASE desapariciones TO data_analyst")
        
        # Grant USAGE on schemas
        cursor.execute("GRANT USAGE ON SCHEMA trusted TO data_analyst")
        cursor.execute("GRANT USAGE ON SCHEMA processed TO data_analyst") # Optional but helpful
        
        # Grant SELECT on all tables
        cursor.execute("GRANT SELECT ON ALL TABLES IN SCHEMA trusted TO data_analyst")
        
        # Default privileges for future tables
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA trusted GRANT SELECT ON TABLES TO data_analyst")
        
        logger.info("Permissions setup completed successfully.")
        
    except Exception as e:
        logger.error(f"Error setting up permissions: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_permissions()
