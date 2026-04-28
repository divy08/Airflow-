from datetime import datetime, timedelta
from airflow import DAG, asset
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from config import S3_BUCKET, S3_PREFIX

TARGET_DAG_ID = 'employee_data_processing'  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG to check for S3 files and trigger employee processing

dag = DAG(    
    's3_trigger_employee_processing',
    default_args=default_args,
    description='Check S3 for employee data files and trigger processing DAG',
    schedule_interval='@hourly',
    # catchup=False,
    tags=['s3', 'employee', 'trigger'],
)

# Sensor to check for any file in the specified S3 prefix
check_s3_file = S3KeySensor(
    task_id='check_employee_data_file_exists',
    bucket_key=f"{S3_PREFIX}*.csv",
    bucket_name=S3_BUCKET,
    aws_conn_id='aws_cred',
    timeout=18 * 60 * 60,       # Timeout after 18 hours
    poke_interval=60 * 5,       # Check every 5 minutes
    mode='poke',
    wildcard_match=True,
    dag=dag,
)

# Trigger the employee data processing DAG
trigger_employee_dag = TriggerDagRunOperator(
    task_id='trigger_employee_data_processing',
    trigger_dag_id=TARGET_DAG_ID,
    execution_date='{{ ds }}',
    reset_dag_run=True,
    wait_for_completion=False,
    dag=dag,
)

check_s3_file >> trigger_employee_dag