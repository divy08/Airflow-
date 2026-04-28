from datetime import datetime, timedelta

# Configuration
S3_BUCKET = 'fug-de-training'
S3_PREFIX = 'dv_prac/raw_data/'
MYSQL_CONN_ID = 'sql_cred'
STAGING_TABLE = 'dv_stag_employee'
FINAL_TABLE = 'dv_main_employee'
ERROR_REPORT_FILENAME = '/tmp/employee_data_errors.xlsx'
TARGET_DAG_ID = 'employee_data_processing'  
AWS_CONN_ID = 'aws_cred'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=1),
}