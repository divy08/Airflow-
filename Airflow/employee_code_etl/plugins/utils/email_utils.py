import logging
import json
import re
import smtplib
import os
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from airflow.models import Variable
from airflow.exceptions import AirflowException

def get_email_config():
    """Get and validate email configuration from Airflow Variables"""
    try:
        raw_value = Variable.get("smtp_config", deserialize_json=False)
        if not raw_value:
            raise ValueError("SMTP configuration not found in Airflow Variables")
            
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
                if not attachment.get('path'):
                    logging.error("Attachment missing path")
                    continue
                    
                if not os.path.exists(attachment['path']):
                    logging.error(f"Attachment file not found: {attachment['path']}")
                    msg.attach(MIMEText(f"\n\nFile not found: {attachment.get('path')}", 'plain'))
                    continue
                
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
        
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.ehlo()
            server.starttls()
            server.login(email_config['sender_email'], email_config['app_password'])
            server.sendmail(email_config['sender_email'], email_config['receiver_emails'], msg.as_string())
        
        logging.info(f"Email sent successfully with subject: {subject}")
        return True
    except Exception as e:
        logging.error(f"Email sending failed: {str(e)}")
        raise AirflowException(f"Email sending failed: {str(e)}")
    
def get_run_id_prefix(dag_run):
        """
        Determine the run_id prefix based on the trigger type.
        """
        if dag_run.run_type == 'manual':
            return 'manual__'
        elif dag_run.run_type == 'scheduled':
            return 'scheduled__'
        else:
            # For other run types like backfill, dataset, etc.
            return f'{dag_run.run_type}__'

def get_task_log_file(task_instance):
    """Get the log file path for a task instance in Docker"""
    
    try:
        dag_id = task_instance.dag_id 
        task_id = task_instance.task_id
        dag_run = task_instance.dag_run
        run_id_prefix = get_run_id_prefix(dag_run)
        execution_date = task_instance.execution_date.strftime("%Y-%m-%dT%H_%M_%S_%f")[:-3]
        # Updated path construction to match your directory structure
        possible_paths = [
            f"/opt/airflow/logs/dag_id={dag_id}/run_id={dag_run.run_id}/task_id={task_id}/attempt=1.log",
            # Older Airflow versions might use this format:
            f"/opt/airflow/logs/dag_id={dag_id}/{dag_run.run_id}/{task_id}/1.log",
        ]
        for log_filepath in possible_paths:
            if os.path.exists(log_filepath):
                return log_filepath
        
        # # Fallback to Docker logs if file not found
        # try:
        #     result = subprocess.run(
        #         ["docker", "ps", "-qf", "name=airflow-worker"],
        #         capture_output=True,
        #         text=True,
        #         check=True
        #     )
        #     container_id = result.stdout.strip()
            
        #     if container_id:
        #         log_result = subprocess.run(
        #             ["docker", "logs", container_id],
        #             capture_output=True,
        #             text=True,
        #             check=True
        #         )
                
        #         if log_result.stdout or log_result.stderr:
        #             temp_log_path = f"/tmp/{dag_id}_{task_id}_docker.log"
        #             with open(temp_log_path, "w") as f:
        #                 if log_result.stdout:
        #                     f.write(log_result.stdout)
        #                 if log_result.stderr:
        #                     f.write("\nSTDERR:\n")
        #                     f.write(log_result.stderr)
        #             return temp_log_path
        # except subprocess.CalledProcessError as docker_err:
        #     logging.warning(f"Could not get Docker logs: {str(docker_err)}")
        # except Exception as docker_err:
        #     logging.warning(f"Unexpected error getting Docker logs: {str(docker_err)}")
        
        logging.warning(f"Could not find log file at any of: {possible_paths}")
        return None
    except Exception as e:
        logging.error(f"Error locating log file: {str(e)}")
        return None

def notify_failure(context):
    """Send failure notification with log file attachment"""
    try:
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
        
        send_email(
            subject=subject,
            body=body,
            attachments=attachments,
            is_error=True
        )
    except Exception as e:
        logging.error(f"Failed to send failure notification: {str(e)}")
        raise