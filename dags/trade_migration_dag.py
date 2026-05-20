"""
Enterprise Trade Data Migration DAG

Orchestrates the ingestion of legacy hive metastore extracts into Snowflake.
This DAG is strictly event-driven (schedule_interval=None), utilizing a FileSensor
to ensure complete dataset landing prior to triggering the Snowflake load sequence.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Load SQL scripts from the sql/ directory assuming standard Airflow template paths
INIT_SQL_PATH = 'sql/01_init.sql'
LOAD_TRANSIENT_SQL_PATH = 'sql/02_load_transient.sql'
MERGE_SQL_PATH = 'sql/03_merge.sql'

with DAG(
    dag_id='enterprise_trade_migration',
    default_args=default_args,
    description='Event-driven ingestion of legacy trades into Snowflake',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['migration', 'snowflake', 'legacy-ingestion'],
    template_searchpath='/opt/airflow/dags/', # Assuming standard path layout for Airflow templates
) as dag:

    # Actively waits for the legacy CSV file to be completely written to the drop zone
    sense_legacy_extract = FileSensor(
        task_id='sense_legacy_extract',
        filepath='legacy_trades.csv',
        fs_conn_id='fs_default',
        poke_interval=60,
        timeout=60 * 60 * 4, # 4 hour timeout waiting for the file
        mode='reschedule',
    )

    # Initialize the Snowflake environment (Stage, Tables)
    initialize_snowflake_env = SnowflakeOperator(
        task_id='initialize_snowflake_env',
        snowflake_conn_id='snowflake_default',
        sql=INIT_SQL_PATH,
    )

    # Load data from the external stage to the transient staging table
    load_transient_stg = SnowflakeOperator(
        task_id='load_transient_stg',
        snowflake_conn_id='snowflake_default',
        sql=LOAD_TRANSIENT_SQL_PATH,
    )

    # Idempotent merge from transient staging to the final target table
    merge_target_trades = SnowflakeOperator(
        task_id='merge_target_trades',
        snowflake_conn_id='snowflake_default',
        sql=MERGE_SQL_PATH,
    )

    # DAG Dependency Chain
    sense_legacy_extract >> initialize_snowflake_env >> load_transient_stg >> merge_target_trades
