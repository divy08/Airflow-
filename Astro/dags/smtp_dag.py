from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models.taskinstance import TaskInstance
from datetime import datetime, timedelta
from helper.error_email_sender import ErrorEmailSender
import pandas as pd
import os

# Email Sender Config
REGION = "ap-south-1"
SOURCE_EMAIL = "divy08.07@gmail.com"
DEST_EMAIL = "divy08.07@gmail.com"

email_client = ErrorEmailSender(REGION, SOURCE_EMAIL, DEST_EMAIL)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    's3_to_db_ses_dag',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
    description='Fetch CSV from S3, insert into MySQL, and notify via SES',
)


def send_failure_email(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    message = f"""
    ❌ Task Failed!
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {context.get('execution_date')}
    Error: {context.get('exception')}
    Log URL: {log_url}
    """
    email_client.send_error_email(message_to_send=message)


def send_success_email(**context):
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    session = context["ti"].get_session()

    message = f"✅ DAG '{dag.dag_id}' completed successfully.\n\nTask Summary:\n"
    for task in dag.tasks:
        ti = (
            session.query(TaskInstance)
            .filter_by(dag_id=dag.dag_id, task_id=task.task_id, run_id=dag_run.run_id)
            .first()
        )
        state = ti.state if ti else "unknown"
        message += f" - {task.task_id}: {state}\n"

    email_client.send_error_email(message_to_send=message)


# -------- S3 Sensor --------
s3_sensor = S3KeySensor(
    task_id='s3_key_sensor',
    bucket_key='dv_prac/raw_data/*.csv',
    bucket_name='fug-de-training',
    aws_conn_id='aws_cred',
    wildcard_match=True,
    timeout=3600,
    poke_interval=60,
    mode='poke',
    dag=dag
)


# -------- Process CSV and generate SQL --------
def process_csv_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_cred')
    bucket = 'fug-de-training'
    prefix = 'dv_prac/raw_data/'

    files = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
    if not files:
        raise ValueError("No CSV files found in S3 path")

    file_key = files[0]
    content = s3_hook.read_key(bucket_name=bucket, key=file_key)

    lines = content.strip().splitlines()
    headers = lines[0].split(',')
    data = [line.split(',') for line in lines[1:]]

    sql_statements = []
    for row in data:
        if len(row) != len(headers):
            continue
        record = dict(zip(headers, row))
        sql_statements.append((
            """
            INSERT INTO dv_customers_new (
                customer_id, first_name, last_name, phone, email,
                street, city, state, zip_code, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (
                record['customer_id'],
                record['first_name'],
                record['last_name'],
                record.get('phone', None),
                record['email'],
                record['street'],
                record['city'],
                record['state'],
                record['zip_code']
            )
        ))
    return sql_statements


process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_csv_from_s3,
    on_failure_callback=send_failure_email,
    dag=dag,
)


# -------- Execute SQL --------
def execute_sql(**kwargs):
    ti = kwargs['ti']
    sqls = ti.xcom_pull(task_ids='process_data')
    if not sqls:
        raise ValueError("No SQL statements returned from processing step")

    mysql = MySqlHook(mysql_conn_id='sql_cred')
    for query, params in sqls:
        mysql.run(query, parameters=params)


execute_sql_task = PythonOperator(
    task_id='execute_sql',
    python_callable=execute_sql,
    on_failure_callback=send_failure_email,
    dag=dag,
)


# -------- Send Success Email --------
success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email,
    trigger_rule='all_success',
    dag=dag,
)


# -------- DAG Dependencies --------
s3_sensor >> process_data >> execute_sql_task >> success_email
