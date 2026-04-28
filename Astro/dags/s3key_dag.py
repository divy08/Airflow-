from airflow import DAG
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

BUCKET_NAME = 'fug-de-training'
SOURCE_KEY_PATTERN = 'dv_prac/raw_data/*.csv'
DESTINATION_PREFIX = 'dv_prac/prep_data/'

def transfer_file_from_s3(**kwargs):
    hook = S3Hook(aws_conn_id='aws_cred')  

    # List keys matching the wildcard pattern
    matched_keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix='dv_prac/raw_data/')
    print(f"Matched keys from S3: {matched_keys}")

    if not matched_keys:
        raise ValueError("No matched keys found in S3")

    for matched_key in matched_keys:
        if matched_key.endswith('.csv'):
            file_obj = hook.get_key(matched_key, bucket_name=BUCKET_NAME)
            content = file_obj.get()['Body'].read()

            filename = os.path.basename(matched_key)
            destination_key = os.path.join(DESTINATION_PREFIX, filename)

            hook.load_bytes(
                bytes_data=content,
                key=destination_key,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            print(f"Transferred {matched_key} to {destination_key}")


with DAG(
    dag_id='s3_file_transfer_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={'retries': 1},
    tags=['s3', 'data-transfer']
) as dag:

    wait_for_file = S3KeySensorAsync(
        task_id='wait_for_s3_file',
        bucket_name=BUCKET_NAME,
        bucket_key=SOURCE_KEY_PATTERN,
        aws_conn_id='aws_cred',
        wildcard_match=True,
        poke_interval=30,
        timeout=600
    )

    transfer_file = PythonOperator(
        task_id='transfer_s3_file',
        python_callable=transfer_file_from_s3
    )

    wait_for_file >> transfer_file
