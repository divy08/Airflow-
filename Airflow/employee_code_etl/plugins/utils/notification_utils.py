import logging
from datetime import datetime
from email_utils import send_email
from config import ERROR_REPORT_FILENAME
from airflow.exceptions import AirflowException

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
        
        if not error_file:
            raise AirflowException("Error file path not found in XCom")
        
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