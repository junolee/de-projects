from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_conn" # Update this to match your Airflow connection ID
WAREHOUSE = "compute_wh"
DATABASE = "airflow_db"
SCHEMA = "bronze"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}
with DAG(
    dag_id='test_snowflake_connection',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # Trigger manually
    catchup=False,
    description='A simple DAG to test Snowflake connection',
    tags=['test', 'snowflake']
) as dag:
    test_snowflake_connection = SnowflakeOperator(
        task_id='run_test_query',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_TIMESTAMP;",
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )