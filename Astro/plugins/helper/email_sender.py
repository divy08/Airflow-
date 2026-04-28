import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging

def send_email_smtp(subject, html_content, to_emails, sender_email, app_password):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ", ".join(to_emails)
        msg['Subject'] = subject
        msg.attach(MIMEText(html_content, 'html'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, app_password)
        server.sendmail(sender_email, to_emails, msg.as_string())
        server.quit()

        logging.info("Email sent successfully via Gmail SMTP.")
    except Exception as e:
        logging.error(f"[SMTP Error] Failed to send email: {e}")
        raise

def send_failure_email_with_context(context, sender_email, app_password, to_emails):
    try:
        dag_id = context.get('dag').dag_id if context.get('dag') else 'Unknown DAG'
        task_instance = context.get('task_instance')
        task_id = task_instance.task_id if task_instance else "Unknown Task"
        execution_date = context.get('execution_date', 'Unknown')
        exception = context.get('exception', 'No exception provided')
        log_url = getattr(task_instance, 'log_url', 'Log URL not available')

        subject = f"[Airflow DAG Failed] DAG: {dag_id}, Task: {task_id}"
        body = f"""
        <html>
        <body>
            <h3 style="color:red;">Airflow Task Failure Alert</h3>
            <ul>
                <li><strong>DAG ID:</strong> {dag_id}</li>
                <li><strong>Task ID:</strong> {task_id}</li>
                <li><strong>Execution Date:</strong> {execution_date}</li>
                <li><strong>Exception:</strong> {exception}</li>
                <li><strong>Log URL:</strong> <a href="{log_url}" target="_blank">{log_url}</a></li>
            </ul>
        </body>
        </html>
        """

        send_email_smtp(subject, body, to_emails, sender_email, app_password)
        logging.info("Failure email sent.")
    except Exception as e:
        logging.error(f"[Email Context Error] Failed to send failure email: {e}")
