
import sys
from pathlib import Path
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.load_raw_csv import main as load_raw_main
from scripts.process_sql_table import main as process_data_main
from scripts.create_views import main as create_views_main
from scripts.setup_db_permissions import setup_permissions as setup_permissions_main

DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
CSV_FILENAME = "base-desapariciones-dataton-2025.csv"
CSV_FILE_PATH = DATA_RAW_DIR / CSV_FILENAME

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'desapariciones_pipeline',
    default_args=default_args,
    description='ETL pipeline for Desapariciones dataset',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'desapariciones'],
)

# Task 1: Check if the dataset exists
check_file_sensor = FileSensor(
    task_id='check_dataset_exists',
    filepath=str(CSV_FILE_PATH),
    poke_interval=30,
    timeout=600,
    mode='poke',
    dag=dag,
)

# Task 2: Load raw CSV to Postgres raw schema
load_raw_task = PythonOperator(
    task_id='load_csv_to_raw',
    python_callable=load_raw_main,
    dag=dag,
)

# Task 3: Process data from raw to processed schema using SQL
process_data_task = PythonOperator(
    task_id='process_raw_to_processed',
    python_callable=process_data_main,
    dag=dag,
)


# Task 4: Create trusted views
create_views_task = PythonOperator(
    task_id='create_trusted_views',
    python_callable=create_views_main,
    dag=dag,
)

# Task 4.5: Setup DB Permissions (Fix for missing role)
setup_permissions_task = PythonOperator(
    task_id='setup_db_permissions',
    python_callable=setup_permissions_main,
    dag=dag,
)

# Task 5: Signal Dashboard Readiness
dashboard_ready = EmptyOperator(
    task_id='dashboard_data_ready',
    dag=dag,
)

# Dependencies
check_file_sensor >> load_raw_task >> process_data_task >>  create_views_task >> setup_permissions_task >> dashboard_ready