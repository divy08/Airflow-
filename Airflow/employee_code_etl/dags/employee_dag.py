from datetime import datetime, timedelta
import pandas as pd
import re
import logging
import uuid
import smtplib
import json
import tempfile
import io
import zipfile
import os
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from io import StringIO
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

# Configuration
S3_BUCKET = 'fug-de-training'
S3_PREFIX = 'dv_prac/raw_data/'
MYSQL_CONN_ID = 'sql_cred'
STAGING_TABLE = 'dv_stab_employee'
FINAL_TABLE = 'dv_main_employee'
ERROR_REPORT_FILENAME = '/tmp/employee_data_errors.xlsx'

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'test_data_processing',
    default_args=default_args,
    description='Process employee data with log file attachments on failure',
    schedule_interval='@daily',
    catchup=False,
    tags=['employee', 'etl'],
)

def get_email_config():
    """Get and validate email configuration from Airflow Variables"""
    try:
        raw_value = Variable.get("smtp_config", deserialize_json=False)
        cleaned_value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', raw_value)
        smtp_config = json.loads(cleaned_value)
        
        required_keys = ['smtp_server', 'smtp_port', 'sender_email', 'receiver_emails', 'app_password']
        for key in required_keys:
            if key not in smtp_config:
                raise ValueError(f"Missing required SMTP config key: {key}")
        
        smtp_config['app_password'] = smtp_config['app_password'].replace(" ", "")
        
        if isinstance(smtp_config['receiver_emails'], str):
            smtp_config['receiver_emails'] = [email.strip() for email in smtp_config['receiver_emails'].split(",")]
            
        return smtp_config
    except Exception as e:
        logging.error(f"Failed to get email config: {str(e)}")
        raise AirflowException(f"Email configuration error: {str(e)}")

def send_email(subject, body, attachments=None, is_error=False):
    """Send email with attachments"""
    try:
        email_config = get_email_config()
        
        msg = MIMEMultipart()
        msg['From'] = email_config['sender_email']
        msg['To'] = ", ".join(email_config['receiver_emails'])
        msg['Subject'] = f"[FAILURE] {subject}" if is_error else subject
        
        msg.attach(MIMEText(body, 'html'))
        
        if attachments:
            for attachment in attachments:
                if attachment.get('path') and os.path.exists(attachment['path']):
                    try:
                        with open(attachment['path'], 'rb') as f:
                            part = MIMEApplication(
                                f.read(),
                                Name=attachment.get('filename', os.path.basename(attachment['path']))
                            )
                        filename = attachment.get('filename', os.path.basename(attachment['path']))
                        part['Content-Disposition'] = f'attachment; filename="{filename}"'
                        msg.attach(part)
                    except Exception as e:
                        logging.error(f"Failed to attach {attachment['path']}: {str(e)}")
                        msg.attach(MIMEText(f"\n\nCould not attach {attachment['path']}", 'plain'))
                else:
                    logging.error(f"Attachment file not found: {attachment.get('path')}")
                    msg.attach(MIMEText(f"\n\nFile not found: {attachment.get('path')}", 'plain'))
        
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.ehlo()
            server.starttls()
            server.login(email_config['sender_email'], email_config['app_password'])
            server.sendmail(email_config['sender_email'], email_config['receiver_emails'], msg.as_string())
        
        return True
    except Exception as e:
        logging.error(f"Email sending failed: {str(e)}")
        raise AirflowException(f"Email sending failed: {str(e)}")

def get_task_log_file(task_instance):
    """Get the log file path for a task instance in Docker"""
    try:
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = task_instance.execution_date.strftime("%Y-%m-%dT%H_%M_%S_%f")[:-3]
        
        # Standard log paths to check
        possible_paths = [
            f"/opt/airflow/logs/{dag_id}/{task_id}/{execution_date}/1.log",
            f"/opt/airflow/logs/{dag_id}/{task_id}/{execution_date}/1.log.1",
        ]
        
        for log_filepath in possible_paths:
            if os.path.exists(log_filepath):
                return log_filepath
        
        # Fallback to Docker logs if file not found
        try:
            result = subprocess.run(
                ["docker", "ps", "-qf", "name=airflow-worker"],
                capture_output=True,
                text=True
            )
            container_id = result.stdout.strip()
            
            if container_id:
                log_result = subprocess.run(
                    ["docker", "logs", container_id],
                    capture_output=True,
                    text=True
                )
                
                if log_result.stdout or log_result.stderr:
                    temp_log_path = f"/tmp/{dag_id}_{task_id}_docker.log"
                    with open(temp_log_path, "w") as f:
                        if log_result.stdout:
                            f.write(log_result.stdout)
                        if log_result.stderr:
                            f.write("\nSTDERR:\n")
                            f.write(log_result.stderr)
                    return temp_log_path
        except Exception as docker_err:
            logging.warning(f"Could not get Docker logs: {str(docker_err)}")
        
        logging.warning(f"Could not find log file at any of: {possible_paths}")
        return None
    except Exception as e:
        logging.error(f"Error locating log file: {str(e)}")
        return None

def notify_failure(context):
    """Send failure notification with log file attachment"""
    task_instance = context['task_instance']
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    
    log_filepath = get_task_log_file(task_instance)
    
    subject = f"Task Failed: {dag_id}.{task_id}"
    body = f"""
    <h3>Airflow Task Failure Notification</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Time:</b> {execution_date}</p>
    <p><b>Exception:</b> <pre>{str(exception)}</pre></p>
    """
    
    attachments = []
    if log_filepath:
        attachments.append({
            'path': log_filepath,
            'filename': f"{dag_id}_{task_id}.log"
        })
        body += "<p>The complete task log is attached to this email.</p>"
    else:
        body += """
        <p style="color: red;">
        Warning: Could not find task log file. Please check Docker logs using:<br>
        <code>docker-compose logs -f worker</code>
        </p>
        """
    
    try:
        send_email(
            subject=subject,
            body=body,
            attachments=attachments,
            is_error=True
        )
    except Exception as e:
        logging.error(f"Failed to send failure notification: {str(e)}")

def generate_unique_billing_code(base_code, existing_codes, firstname, lastname, emp_id):
    """Generate unique billing code with conflict resolution"""
    if base_code not in existing_codes:
        return base_code
    
    patterns = [
        {'serial_range': (1, 9), 'firstname_chars': 2, 'lastname_chars': 3},
        {'serial_range': (10, 99), 'firstname_chars': 2, 'lastname_chars': 2},
        {'serial_range': (100, 999), 'firstname_chars': 2, 'lastname_chars': 1},
        {'serial_range': (1000, 9999), 'firstname_chars': 2, 'lastname_chars': 0},
        {'serial_range': (10000, 99999), 'firstname_chars': 1, 'lastname_chars': 0},
        {'serial_range': (100000, 999999), 'firstname_chars': 0, 'lastname_chars': 0},
        {'serial_range': (1000000, 9999999), 'pattern': lambda i, fn, ln, eid: f"{i}{eid}"},
        {'serial_range': (10000000, 99999999), 'pattern': lambda i, fn, ln, eid: f"X{i}{eid}"}
    ]

    for pattern in patterns:
        start, end = pattern['serial_range']
        for i in range(start, end + 1):
            if 'pattern' in pattern:
                candidate = pattern['pattern'](i, firstname, lastname, emp_id)
            else:
                fn_part = firstname[:pattern['firstname_chars']] if pattern['firstname_chars'] > 0 else ""
                ln_part = lastname[:pattern['lastname_chars']] if pattern['lastname_chars'] > 0 else ""
                candidate = f"{i}{fn_part}{ln_part}{emp_id}"
            
            if candidate not in existing_codes:
                return candidate
    
    return f"UID{uuid.uuid4().hex[:6]}"

def extract_data_from_s3(**kwargs):
    """Extract CSV data from S3 bucket"""
    try:
        s3_hook = S3Hook(aws_conn_id='aws_cred')
        files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
        if not files:
            raise AirflowException(f"No files found in s3://{S3_BUCKET}/{S3_PREFIX}")
        
        csv_file = next((f for f in files if f.lower().endswith('.csv')), None)
        if not csv_file:
            raise AirflowException(f"No CSV files found in s3://{S3_BUCKET}/{S3_PREFIX}")
        
        file_obj = s3_hook.get_key(bucket_name=S3_BUCKET, key=csv_file)
        csv_content = file_obj.get()['Body'].read().decode('utf-8')
        
        kwargs['ti'].xcom_push(key='csv_content', value=csv_content)
        kwargs['ti'].xcom_push(key='source_filename', value=csv_file)
        logging.info(f"Extracted {csv_file} from S3")
        return csv_content
    except Exception as e:
        logging.error(f"S3 extraction failed: {str(e)}")
        raise AirflowException(f"S3 extraction failed: {str(e)}")

def validate_and_stage_records(**kwargs):
    """Validate records and stage in MySQL"""
    try:
        ti = kwargs['ti']
        csv_content = ti.xcom_pull(task_ids='extract_data_from_s3', key='csv_content')
        source_file = ti.xcom_pull(task_ids='extract_data_from_s3', key='source_filename')
        df = pd.read_csv(StringIO(csv_content))
        
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT billing_code FROM {FINAL_TABLE}")
        existing_codes = {row[0] for row in cursor.fetchall()}
        
        cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        
        valid_records = []
        invalid_records = []
        duplicate_records = []
        staging_inserts = []
        
        for _, row in df.iterrows():
            cursor.execute(
                f"SELECT 1 FROM {FINAL_TABLE} WHERE employee_id = %s AND firstname = %s AND lastname = %s AND salary = %s AND email = %s LIMIT 1",
                (row['employee_id'], row['firstname'], row['lastname'], float(row['salary']), row['email'])
            )
            
            if cursor.fetchone():
                duplicate_records.append({
                    **row.to_dict(),
                    'source_file': source_file,
                    'error_reason': 'Duplicate record in final table'
                })
                continue
            
            errors = []
            if not (isinstance(row['employee_id'], int) and 1000 <= row['employee_id'] <= 9999):
                errors.append("Invalid employee_id (must be 4-digit integer)")
            if not row['firstname'] or len(str(row['firstname'])) > 20:
                errors.append("Invalid firstname (required, max 20 chars)")
            if not row['lastname'] or len(str(row['lastname'])) > 20:
                errors.append("Invalid lastname (required, max 20 chars)")
            try:
                float(row['salary'])
            except (ValueError, TypeError):
                errors.append("Invalid salary (must be numeric)")
            if not re.match(r"[^@]+@[^@]+\.[^@]+", str(row['email'])):
                errors.append("Invalid email format")
            
            if errors:
                invalid_records.append({
                    **row.to_dict(),
                    'source_file': source_file,
                    'error_reason': '; '.join(errors)
                })
                continue
            
            firstname = str(row['firstname']).strip()
            lastname = str(row['lastname']).strip()
            emp_id = str(row['employee_id'])
            base_code = f"{firstname[:2]}{lastname[:4]}{emp_id}"
            
            billing_code = generate_unique_billing_code(
                base_code=base_code,
                existing_codes=existing_codes,
                firstname=firstname,
                lastname=lastname,
                emp_id=emp_id
            )
            
            resource_id = billing_code[:6]
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            staging_inserts.append((
                row['employee_id'], row['firstname'], row['lastname'],
                float(row['salary']), row['email'], billing_code,
                resource_id, created_at
            ))
            
            valid_records.append(row.to_dict())
            existing_codes.add(billing_code)
        
        if staging_inserts:
            cursor.executemany(
                f"""
                INSERT INTO {STAGING_TABLE} 
                (employee_id, firstname, lastname, salary, email, billing_code, resource_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                staging_inserts
            )
        
        if invalid_records or duplicate_records:
            error_df = pd.DataFrame(invalid_records + duplicate_records)
            error_df.to_excel(ERROR_REPORT_FILENAME, index=False)
            ti.xcom_push(key='error_file', value=ERROR_REPORT_FILENAME)
            ti.xcom_push(key='error_count', value=len(invalid_records) + len(duplicate_records))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Processed: {len(valid_records)} valid, {len(invalid_records)} invalid, {len(duplicate_records)} duplicates")
        return len(valid_records)
    except Exception as e:
        logging.error(f"Validation and staging failed: {str(e)}")
        raise AirflowException(f"Validation and staging failed: {str(e)}")

def send_error_report(**kwargs):
    """Send error report email with attachment"""
    try:
        ti = kwargs['ti']
        error_count = ti.xcom_pull(task_ids='validate_and_stage_records', key='error_count', default=0)
        
        if error_count == 0:
            logging.info("No errors found, skipping error report")
            return True
        
        error_file = ti.xcom_pull(task_ids='validate_and_stage_records', key='error_file')
        source_file = ti.xcom_pull(task_ids='extract_data_from_s3', key='source_filename')
        
        subject = f"Employee Data Errors - {datetime.now().strftime('%Y-%m-%d')}"
        body = f"""
        <h3>Employee Data Processing Error Report</h3>
        <p><b>Source File:</b> {source_file}</p>
        <p><b>Total Errors:</b> {error_count}</p>
        <p>See attached Excel file for details.</p>
        """
        
        attachments = [{'path': error_file, 'filename': 'employee_data_errors.xlsx'}]
        return send_email(subject, body, attachments=attachments)
    except Exception as e:
        logging.error(f"Error report failed: {str(e)}")
        raise AirflowException(f"Error report failed: {str(e)}")

def load_to_final_table(**kwargs):
    """Load valid records from staging to final table"""
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # First verify the table structure
        cursor.execute(f"DESCRIBE {FINAL_TABLE}")
        columns = [col[0] for col in cursor.fetchall()]
        
        if 'email' not in columns:
            raise AirflowException(f"Table {FINAL_TABLE} is missing the 'email' column")
        
        # Now perform the insert with correct column names
        cursor.execute(
            f"""
            INSERT INTO {FINAL_TABLE} 
            (employee_id, firstname, lastname, salary, email, billing_code, resource_id, created_at)
            SELECT 
                employee_id, firstname, lastname, salary, email,
                billing_code, resource_id, created_at
            FROM {STAGING_TABLE}
            ON DUPLICATE KEY UPDATE
                firstname = VALUES(firstname),
                lastname = VALUES(lastname),
                salary = VALUES(salary),
                billing_code = VALUES(billing_code),
                resource_id = VALUES(resource_id)
            """
        )
        
        affected_rows = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Loaded {affected_rows} records to final table")
        return affected_rows
    except Exception as e:
        logging.error(f"Final table load failed: {str(e)}")
        raise AirflowException(f"Final table load failed: {str(e)}")

def cleanup_staging(**kwargs):
    """Clean up staging table"""
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Staging table cleaned up")
        return True
    except Exception as e:
        logging.error(f"Cleanup failed: {str(e)}")
        raise AirflowException(f"Cleanup failed: {str(e)}")
    
def archive_source_file(**kwargs):
    """Archive source file by moving it to prep_data folder in zip format"""
    try:
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_cred')
        
        # Get the source filename from previous task
        source_file = ti.xcom_pull(task_ids='extract_data_from_s3', key='source_filename')
        if not source_file:
            logging.warning("No source file found to archive")
            return False
        
        # Generate timestamp and new filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(source_file))[0]
        zip_filename = f"{base_name}_{timestamp}.zip"
        prep_path = f"dv_prac/prep_data/{zip_filename}"
        
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file_path = os.path.join(tmp_dir, os.path.basename(source_file))
            local_zip_path = os.path.join(tmp_dir, zip_filename)
            
            try:
                # Download the file from S3
                logging.info(f"Downloading {source_file} from S3")
                file_content = s3_hook.read_key(
                    key=source_file,
                    bucket_name=S3_BUCKET
                )
                
                # Write content to local file
                with open(local_file_path, 'w') as f:
                    f.write(file_content)
                
                # Create zip file with the actual content
                logging.info(f"Creating ZIP archive at {local_zip_path}")
                with zipfile.ZipFile(local_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(
                        local_file_path,
                        arcname=os.path.basename(source_file)
                    )
                
                # Verify the ZIP contains our file
                with zipfile.ZipFile(local_zip_path, 'r') as zipf:
                    file_list = zipf.namelist()
                    if os.path.basename(source_file) not in file_list:
                        raise AirflowException("Failed to add file to ZIP archive")
                    file_size = zipf.getinfo(os.path.basename(source_file)).file_size
                    if file_size == 0:
                        raise AirflowException("ZIP contains empty file")
                
                # Upload the zip file to S3
                logging.info(f"Uploading ZIP to {prep_path}")
                s3_hook.load_file(
                    filename=local_zip_path,
                    key=prep_path,
                    bucket_name=S3_BUCKET,
                    replace=True
                )
                
                # Delete the original file
                logging.info(f"Deleting original file {source_file}")
                s3_hook.delete_objects(bucket=S3_BUCKET, keys=source_file)
                
                logging.info(f"Successfully moved {source_file} to {prep_path}")
                return True
                
            except Exception as e:
                logging.error(f"Error during file processing: {str(e)}")
                raise AirflowException(f"File processing failed: {str(e)}")
                
    except Exception as e:
        logging.error(f"File archiving failed: {str(e)}")
        raise AirflowException(f"File archiving failed: {str(e)}")
    

def send_success_notification(**kwargs):
    """Send success notification email"""
    try:
        ti = kwargs['ti']
        
        staged_count = ti.xcom_pull(task_ids='validate_and_stage_records', default=0)
        error_count = ti.xcom_pull(task_ids='validate_and_stage_records', key='error_count', default=0)
        loaded_count = ti.xcom_pull(task_ids='load_to_final_table', default=0)
        source_file = ti.xcom_pull(task_ids='extract_data_from_s3', key='source_filename', default='unknown')
        
        subject = f"Employee Data Processed - {datetime.now().strftime('%Y-%m-%d')}"
        body = f"""
        <h3>Employee Data Processing Complete</h3>
        <p><b>Source File:</b> {source_file}</p>
        <p><b>Valid Records Processed:</b> {staged_count}</p>
        <p><b>Errors Found:</b> {error_count}</p>
        <p><b>Records Loaded:</b> {loaded_count}</p>
        """
        
        return send_email(subject, body)
    except Exception as e:
        logging.error(f"Success notification failed: {str(e)}")
        raise AirflowException(f"Notification failed: {str(e)}")



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