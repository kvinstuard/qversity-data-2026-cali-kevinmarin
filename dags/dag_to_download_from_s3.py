from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json


truncate_query = """TRUNCATE bronze.mobile_customers_raw RESTART IDENTITY;"""

insert_query = """
        INSERT INTO bronze.mobile_customers_raw (raw_payload, ingestion_timestamp)
        VALUES (%s, %s)
    """

def download_from_s3(**context):
    hook = S3Hook(aws_conn_id='public_s3_qbika')
    execution_ts = context['data_interval_start']

    obj = hook.get_key(
    key='mobile_customers_messy_dataset.json',
    bucket_name='qversity-raw-public-data'
    )

    content = obj.get()['Body'].read().decode('utf-8')
    data = json.loads(content)

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    records = [(json.dumps(record), execution_ts) for record in data]

    cursor.execute(truncate_query)
    cursor.executemany(insert_query, records)
    conn.commit()

    cursor.close()
    conn.close()



default_args = {
    'owner': 'kevin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_download_from_s3_v01',
    default_args=default_args,
    start_date=datetime(2026, 2, 24),
    schedule='0 0 * * *',
    catchup=False
) as dag:

    wait_for_file = S3KeySensor(
        task_id='s3_sensor',
        bucket_name='qversity-raw-public-data',
        bucket_key='mobile_customers_messy_dataset.json',
        aws_conn_id='public_s3_qbika',
        poke_interval=60,
        timeout=600,
        mode="reschedule"
    )

    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_from_s3
    )

    wait_for_file >> download_task