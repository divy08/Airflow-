from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
# from config import default_args
from email_utils import notify_failure
from data_utils import (
    extract_data_from_s3,
    validate_and_stage_records,
    load_to_final_table,
    cleanup_staging
)
from file_utils import archive_source_file
from notification_utils import (
    send_error_report,
    send_success_notification
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'employee_data_processing',
    default_args=default_args,
    description='Process employee data with log file attachments on failure',
    schedule_interval=None,
    catchup=False,
    tags=['employee', 'etl'],
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data_from_s3',
    python_callable=extract_data_from_s3,
    on_failure_callback=notify_failure,
    dag=dag,
)

validate_stage_task = PythonOperator(
    task_id='validate_and_stage_records',
    python_callable=validate_and_stage_records,
    on_failure_callback=notify_failure,
    dag=dag,
)

error_report_task = PythonOperator(
    task_id='send_error_report',
    python_callable=send_error_report,
    on_failure_callback=notify_failure,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_final_table',
    python_callable=load_to_final_table,
    on_failure_callback=notify_failure,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_staging',
    python_callable=cleanup_staging,
    on_failure_callback=notify_failure,
    dag=dag,
)

archive_task = PythonOperator(
    task_id='archive_source_file',
    python_callable=archive_source_file,
    on_failure_callback=notify_failure,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    on_failure_callback=notify_failure,
    dag=dag,
)

# Set workflow
extract_task >> validate_stage_task >> [error_report_task, load_task] >> cleanup_task >> archive_task >> notify_task