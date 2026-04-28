from datetime import datetime, timezone
from typing import Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pandas as pd
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

def send_email_notification(context: Dict[str, Any], status: str, error_msg: Optional[str] = None) -> None:
    """
    Enhanced email notification with proper context handling
    """
    try:
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Safely get context values with defaults
        task_id = context.get('task_instance', {}).task_id if 'task_instance' in context else context.get('task', {}).task_id
        dag_id = context.get('dag', {}).dag_id if 'dag' in context else 'unknown_dag'
        execution_date = context.get('execution_date', 'unknown_execution_date')
        
        # Get SMTP configuration safely
        try:
            smtp_config = Variable.get('smtp_config', deserialize_json=True)
            sender = smtp_config.get('sender_email', 'no-reply@example.com')
            recipients = smtp_config.get('receiver_emails', [])
            app_password = smtp_config.get('app_password', '')
        except Exception as e:
            logger.error(f"Failed to get SMTP config: {str(e)}")
            raise

        # Create email subject and body
        subject = f"DAG {dag_id} - Task {task_id} - {status.upper()}"
        
        body = f"""
        <html>
            <body>
                <h2>Airflow Task Notification</h2>
                <table border="1" cellpadding="5" cellspacing="0">
                    <tr><th>DAG Name</th><td>{dag_id}</td></tr>
                    <tr><th>Task ID</th><td>{task_id}</td></tr>
                    <tr><th>Status</th><td><strong>{status.upper()}</strong></td></tr>
                    <tr><th>Execution Time</th><td>{execution_date}</td></tr>
                    <tr><th>Notification Time</th><td>{current_time}</td></tr>
        """
        
        if status == 'failure' and error_msg:
            body += f"""<tr><th>Error Details</th><td style="color:red">{error_msg}</td></tr>"""
        
        body += """
                </table>
                <p>This is an automated notification from Airflow.</p>
            </body>
        </html>
        """

        # Only send email if recipients are configured
        if recipients:
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            msg.attach(MIMEText(body, 'html'))

            try:
                with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
                    smtp_server.login(sender, app_password)
                    smtp_server.sendmail(sender, recipients, msg.as_string())
                logger.info(f"Email notification sent for {task_id}")
            except Exception as e:
                logger.error(f"Failed to send email: {str(e)}")
                raise
        else:
            logger.warning("No email recipients configured - skipping notification")
    except Exception as e:
        logger.error(f"Failed to prepare notification: {str(e)}")
        raise

def verify_s3_connection() -> bool:
    """Verify S3 connection and list bucket contents"""
    try:
        s3_hook = S3Hook(aws_conn_id='aws_cred')
        bucket_name = 'fug-de-training'
        prefix = 'dv_prac/raw_data/'
        
        # Check if bucket exists
        if not s3_hook.check_for_bucket(bucket_name):
            logger.error(f"Bucket {bucket_name} does not exist or is not accessible")
            return False
        
        # List files in prefix
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        logger.info(f"Found {len(keys)} keys in s3://{bucket_name}/{prefix}")
        
        if keys:
            logger.info(f"First 5 keys: {keys[:5]}")
        
        # Check for actual CSV files (not just directories)
        csv_files = [k for k in keys if k.lower().endswith('.csv')]
        logger.info(f"Found {len(csv_files)} CSV files")
        
        return len(csv_files) > 0
    except Exception as e:
        logger.error(f"S3 connection failed: {str(e)}")
        raise

def pull_csv_from_s3(**context) -> None:
    """Pull CSV files from S3 with enhanced error handling"""
    try:
        logger.info("Starting S3 file processing")
        ti = context.get('ti')
        
        # Verify S3 connection and files
        if not verify_s3_connection():
            error_msg = "No CSV files found or connection failed"
            logger.error(error_msg)
            send_email_notification(context, 'failure', error_msg)
            raise ValueError(error_msg)

        s3_hook = S3Hook(aws_conn_id='aws_cred')
        bucket_name = 'fug-de-training'
        prefix = 'dv_prac/raw_data/'

        # Get all CSV files
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        csv_keys = [k for k in keys if k.lower().endswith('.csv')]

        if not csv_keys:
            error_msg = f"No CSV files found in s3://{bucket_name}/{prefix}"
            logger.error(error_msg)
            send_email_notification(context, 'failure', error_msg)
            raise ValueError(error_msg)

        # Process files
        dfs = []
        for key in csv_keys:
            try:
                logger.info(f"Processing {key}")
                file_obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
                csv_content = file_obj.get()['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(csv_content))
                logger.info(f"Loaded {len(df)} rows from {key}")
                dfs.append(df)
            except Exception as e:
                error_msg = f"Failed to process {key}: {str(e)}"
                logger.error(error_msg)
                raise

        # Combine and push to XCom
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_csv = combined_df.to_csv(index=False)
        if ti:
            ti.xcom_push(key='csv_content', value=combined_csv)
        
        success_msg = f"Processed {len(csv_keys)} files ({len(combined_df)} rows)"
        logger.info(success_msg)
        send_email_notification(context, 'success', success_msg)
    except Exception as e:
        error_msg = f"S3 processing failed: {str(e)}"
        logger.error(error_msg)
        send_email_notification(context, 'failure', error_msg)
        raise

def load_to_mysql(**context) -> None:
    """Load data to MySQL with error handling"""
    try:
        logger.info("Starting MySQL load")
        ti = context.get('ti')
        
        # Get data from XCom
        csv_content = ti.xcom_pull(key='csv_content', task_ids='pull_csv_task') if ti else None
        if not csv_content:
            error_msg = "No CSV content found in XCom"
            logger.error(error_msg)
            send_email_notification(context, 'failure', error_msg)
            raise ValueError(error_msg)
            
        # Process data
        df = pd.read_csv(io.StringIO(csv_content))
        df['created_at'] = pd.Timestamp.now()
        logger.info(f"Preparing to load {len(df)} rows")

        # Load to MySQL in chunks
        mysql_hook = MySqlHook(mysql_conn_id='sql_cred')
        engine = mysql_hook.get_sqlalchemy_engine()
        
        chunksize = 1000
        total_rows = 0
        for i in range(0, len(df), chunksize):
            chunk = df[i:i + chunksize]
            chunk.to_sql('dv_customers_new', engine, if_exists='append', index=False)
            total_rows += len(chunk)
            logger.info(f"Loaded chunk {i//chunksize + 1}: {len(chunk)} rows")

        success_msg = f"Successfully loaded {total_rows} rows to MySQL"
        logger.info(success_msg)
        send_email_notification(context, 'success', success_msg)
    except Exception as e:
        error_msg = f"MySQL load failed: {str(e)}"
        logger.error(error_msg)
        send_email_notification(context, 'failure', error_msg)
        raise

with DAG(
    dag_id='s3_wildcard_to_mysql_dag',
    default_args=default_args,
    description='Robust S3 to MySQL pipeline with error handling',
    schedule=None,
    start_date=datetime(2025, 6, 10),
    catchup=False,
    tags=['s3', 'mysql', 'data-pipeline'],
) as dag:

    check_s3_file = S3KeySensor(
        task_id='check_s3_file',
        bucket_name='fug-de-training',
        bucket_key='dv_prac/raw_data/',
        aws_conn_id='aws_cred',
        timeout=600,  # 10 minutes
        poke_interval=30,
        mode='poke',
        wildcard_match=True,
    )

    pull_csv_task = PythonOperator(
        task_id='pull_csv_task',
        python_callable=pull_csv_from_s3,
    )

    load_mysql_task = PythonOperator(
        task_id='load_mysql_task',
        python_callable=load_to_mysql,
    )

    check_s3_file >> pull_csv_task >> load_mysql_task