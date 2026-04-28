# from airflow import DAG
# from datetime import datetime, timedelta
# from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.amazon.aws.hooks.ses import SesHook
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# import logging

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime.now() - timedelta(days=1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     's3_to_db_email_workflow_with_timestamp',
#     default_args=default_args,
#     description='A workflow that pulls customer data from S3 and dumps it into a database with a timestamp',
#     schedule=timedelta(days=1),
# )

# # Sensor to wait for the S3 key
# s3_sensor = S3KeySensor(
#     task_id='s3_key_sensor',
#     bucket_key='dv_prac/raw_data/*.csv',
#     bucket_name='fug-de-training',
#     aws_conn_id='aws_cred',
#     wildcard_match=True,
#     timeout=18 * 60 * 60,
#     poke_interval=120,
#     dag=dag,
# )

# def process_data_and_generate_sql(**kwargs):
#     s3_hook = S3Hook(aws_conn_id='aws_cred')
#     bucket_name = 'fug-de-training'
#     prefix = 'dv_prac/raw_data/'

#     # List and filter files based on the prefix
#     file_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
#     logging.info(f"Found files: {file_keys}")

#     if not file_keys:
#         raise ValueError(f"No files found in {bucket_name} with prefix {prefix}")

#     # Process the first file found
#     key = file_keys[1]  # Assuming the second item is the file we want to process
#     logging.info(f"Reading file: {key}")

#     try:
#         # Read the CSV file from S3
#         data = s3_hook.read_key(key=key, bucket_name=bucket_name)
#         logging.info(f"File content preview: {data[:200]}")  # Log a preview of the file content
#     except Exception as e:
#         logging.error(f"Failed to read key {key} from bucket {bucket_name}: {e}")
#         raise

#     # Check if the file is empty
#     if not data.strip():
#         raise ValueError(f"The file {key} is empty")

#     # Process data and generate SQL with timestamp
#     sql_statements = []
#     lines = data.splitlines()
#     if len(lines) < 2:  # Ensure there is at least a header and one data line
#         raise ValueError("The file does not contain sufficient data lines")

#     headers = lines[0].split(',')
#     logging.info(f"Headers: {headers}")  # Log the headers for debugging

#     for record in lines[1:]:
#         processed_record = process_record(record)
#         logging.info(f"Processed record: {processed_record}")  # Log each processed record
#         if len(processed_record) != len(headers):
#             logging.error(f"Record {record} does not match headers {headers}")
#             continue

#         record_dict = dict(zip(headers, processed_record))
#         # Use parameterized queries to avoid SQL injection and syntax errors
#         sql = """
#         INSERT INTO dv_customers_new (customer_id, first_name, last_name, phone, email, street, city, state, zip_code, timestamp)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
#         """
#         # Collect parameters for the parameterized query
#         params = (
#             record_dict['customer_id'],
#             record_dict['first_name'],
#             record_dict['last_name'],
#             record_dict['phone'] if record_dict.get('phone') else None,
#             record_dict['email'],
#             record_dict['street'],
#             record_dict['city'],
#             record_dict['state'],
#             record_dict['zip_code']
#         )
#         sql_statements.append((sql, params))

#     return sql_statements

# def process_record(record):
#     return [value.strip() for value in record.split(',')]

# def send_email_via_ses(subject, html_content, to_email):
#     ses_hook = SesHook(aws_conn_id='aws_cred')
#     response = ses_hook.send_email(
#         mail_from='divy08.07@gmail.com',
#         to=to_email,
#         subject=subject,
#         html_content=html_content
#     )
#     return response

# process_data_task = PythonOperator(
#     task_id='process_data',
#     python_callable=process_data_and_generate_sql,
#     dag=dag,
# )

# def execute_sql_queries(**kwargs):
#     ti = kwargs['ti']
#     sql_statements = ti.xcom_pull(task_ids='process_data')
#     mysql_hook = MySqlHook(mysql_conn_id='sql_cred')

#     for sql, params in sql_statements:
#         mysql_hook.run(sql, parameters=params)

# execute_sql_task = PythonOperator(
#     task_id='execute_sql',
#     python_callable=execute_sql_queries,
#     dag=dag,
# )

# send_success_email_task = PythonOperator(
#     task_id='send_success_email',
#     python_callable=send_email_via_ses,
#     op_kwargs={
#         'subject': 'Workflow Success',
#         'html_content': '<p>The workflow has completed successfully.</p>',
#         'to_email': 'divy08.07@gmail.com'
#     },
#     dag=dag,
# )

# send_failure_email_task = PythonOperator(
#     task_id='send_failure_email',
#     python_callable=send_email_via_ses,
#     op_kwargs={
#         'subject': 'Workflow Failed',
#         'html_content': '<p>The workflow has failed after retries.</p>',
#         'to_email': 'divy08.07@gmail.com'
#     },
#     trigger_rule='all_failed',
#     dag=dag,
# )

# s3_sensor >> process_data_task >> execute_sql_task >> send_success_email_task
# execute_sql_task >> send_failure_email_task



# from airflow import DAG
# from datetime import datetime, timedelta
# from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.amazon.aws.hooks.ses import SesHook
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from helper.error_email_sender import ErrorEmailSender, CustomException
# import logging

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime.now() - timedelta(days=1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     's3_to_db_email_workflow_with_timestamp',
#     default_args=default_args,
#     description='A workflow that pulls customer data from S3 and dumps it into a database with a timestamp',
#     schedule=timedelta(days=1),
# )

# # Sensor to wait for the S3 key
# s3_sensor = S3KeySensor(
#     task_id='s3_key_sensor',
#     bucket_key='dv_prac/raw_data/*.csv',
#     bucket_name='fug-de-training',
#     aws_conn_id='aws_cred',
#     wildcard_match=True,
#     timeout=18 * 60 * 60,
#     poke_interval=120,
#     dag=dag,
# )

# def process_data_and_generate_sql(**kwargs):
#     try:
#         s3_hook = S3Hook(aws_conn_id='aws_cred')
#         bucket_name = 'fug-de-training'
#         prefix = 'dv_prac/raw_data/'

#         file_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
#         logging.info(f"Found files: {file_keys}")

#         if not file_keys:
#             raise ValueError(f"No files found in {bucket_name} with prefix {prefix}")

#         key = file_keys[1]
#         logging.info(f"Reading file: {key}")

#         data = s3_hook.read_key(key=key, bucket_name=bucket_name)
#         logging.info(f"File content preview: {data[:200]}")

#         if not data.strip():
#             raise ValueError(f"The file {key} is empty")

#         sql_statements = []
#         lines = data.splitlines()
#         if len(lines) < 2:
#             raise ValueError("The file does not contain sufficient data lines")

#         headers = lines[0].split(',')
#         logging.info(f"Headers: {headers}")

#         for record in lines[1:]:
#             processed_record = [value.strip() for value in record.split(',')]
#             logging.info(f"Processed record: {processed_record}")

#             if len(processed_record) != len(headers):
#                 logging.error(f"Record {record} does not match headers {headers}")
#                 continue

#             record_dict = dict(zip(headers, processed_record))
#             sql = """
#             INSERT INTO dv_customers_new (customer_id, first_name, last_name, phone, email, street, city, state, zip_code, timestamp)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
#             """
#             params = (
#                 record_dict['customer_id'],
#                 record_dict['first_name'],
#                 record_dict['last_name'],
#                 record_dict['phone'] if record_dict.get('phone') else None,
#                 record_dict['email'],
#                 record_dict['street'],
#                 record_dict['city'],
#                 record_dict['state'],
#                 record_dict['zip_code']
#             )
#             sql_statements.append((sql, params))

#         return sql_statements

#     except Exception as e:
#         error_email_sender = ErrorEmailSender(
#             region_name="us-west-2",
#             source_email="divy08.07@gmail.com",
#             destination_email="divy08.07@gmail.com"
#         )
#         error_message = str(e)
#         error_email_sender.send_error_email(error_message)
#         raise

# def execute_sql_queries(**kwargs):
#     try:
#         ti = kwargs['ti']
#         sql_statements = ti.xcom_pull(task_ids='process_data')
#         mysql_hook = MySqlHook(mysql_conn_id='sql_cred')

#         for sql, params in sql_statements:
#             mysql_hook.run(sql, parameters=params)

#     except Exception as e:
#         error_email_sender = ErrorEmailSender(
#             region_name="us-west-2",
#             source_email="divy08.07@gmail.com",
#             destination_email="divy08.07@gmail.com"
#         )
#         error_message = str(e)
#         error_email_sender.send_error_email(error_message)
#         raise

# process_data_task = PythonOperator(
#     task_id='process_data',
#     python_callable=process_data_and_generate_sql,
#     dag=dag,
# )

# execute_sql_task = PythonOperator(
#     task_id='execute_sql',
#     python_callable=execute_sql_queries,
#     dag=dag,
# )

# def send_email_via_ses(subject, html_content, to_email):
#     ses_hook = SesHook(aws_conn_id='aws_cred')
#     response = ses_hook.send_email(
#         mail_from='divy08.07@gmail.com',
#         to=to_email,
#         subject=subject,
#         html_content=html_content
#     )
#     return response

# send_success_email_task = PythonOperator(
#     task_id='send_success_email',
#     python_callable=send_email_via_ses,
#     op_kwargs={
#         'subject': 'Workflow Success',
#         'html_content': '<p>The workflow has completed successfully.</p>',
#         'to_email': 'divy08.07@gmail.com'
#     },
#     dag=dag,
# )

# send_failure_email_task = PythonOperator(
#     task_id='send_failure_email',
#     python_callable=send_email_via_ses,
#     op_kwargs={
#         'subject': 'Workflow Failed',
#         'html_content': '<p>The workflow has failed after retries.</p>',
#         'to_email': 'divy08.07@gmail.com'
#     },
#     trigger_rule='all_failed',
#     dag=dag,
# )

# s3_sensor >> process_data_task >> execute_sql_task >> send_success_email_task
# execute_sql_task >> send_failure_email_task


# from airflow import DAG
# from airflow.models import Variable
# from datetime import datetime, timedelta
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensor
# import logging
# import json
# from helper.email_sender import send_email_smtp, send_failure_email_with_context

# # Load SMTP config from Airflow Variable
# smtp_config_json = Variable.get("smtp_config")
# smtp_config = json.loads(smtp_config_json)

# SMTP_SENDER_EMAIL = smtp_config['sender_email']
# SMTP_APP_PASSWORD = smtp_config['app_password']
# SMTP_RECEIVER_EMAIL = smtp_config['receiver_emails']

# # ------------------ Email Callback ------------------
# def failure_callback(context):
#     send_failure_email_with_context(
#         context=context,
#         sender_email=SMTP_SENDER_EMAIL,
#         app_password=SMTP_APP_PASSWORD,
#         to_emails=SMTP_RECEIVER_EMAIL
#     )

# # ------------------ Default Args ------------------
# def_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime.now() - timedelta(days=1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=3),
#     'on_failure_callback': failure_callback
# }

# dag = DAG(
#     's3_to_db_email_workflow_with_smtp',
#     default_args=def_args,
#     description='Pulls CSV from S3, inserts into MySQL, and sends SMTP emails',
#     schedule=timedelta(days=1),
#     catchup=False
# )

# # ------------------ Task: S3 Sensor ------------------
# s3_sensor = S3KeySensor(
#     task_id='s3_key_sensor',
#     bucket_key='dv_prac/raw_data/*.csv',
#     bucket_name='fug-de-training',
#     aws_conn_id='aws_cred',
#     wildcard_match=True,
#     timeout=3600,
#     poke_interval=60,
#     dag=dag,
# )

# # ------------------ Task: Process Data ------------------
# def process_data_and_generate_sql(**kwargs):
#     s3_hook = S3Hook(aws_conn_id='aws_cred')
#     bucket = 'fug-de-training'
#     prefix = 'dv_prac/raw_data/'

#     file_keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
#     logging.info(f"Files found: {file_keys}")

#     if not file_keys:
#         raise ValueError(f"No files found at {prefix}")

#     key = file_keys[1]
#     content = s3_hook.read_key(bucket_name=bucket, key=key)

#     if not content.strip():
#         raise ValueError(f"{key} is empty")

#     lines = content.splitlines()
#     headers = lines[0].split(',')
#     sqls = []

#     for line in lines[1:]:
#         values = line.split(',')
#         if len(values) != len(headers):
#             logging.warning(f"Skipping: {line}")
#             continue
#         row = dict(zip(headers, values))
#         sqls.append((
#             """
#             INSERT INTO dv_customers_new (
#                 customer_id, first_name, last_name, phone, email,
#                 street, city, state, zip_code, timestamp
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
#             """,
#             (
#                 row['customer_id'],
#                 row['first_name'],
#                 row['last_name'],
#                 row['phone'] or None,
#                 row['email'],
#                 row['street'],
#                 row['city'],
#                 row['state'],
#                 row['zip_code']
#             )
#         ))

#     return sqls

# process_data_task = PythonOperator(
#     task_id='process_data',
#     python_callable=process_data_and_generate_sql,
#     dag=dag,
# )

# # ------------------ Task: Execute SQL ------------------
# def execute_sql_queries(**kwargs):
#     ti = kwargs['ti']
#     sqls = ti.xcom_pull(task_ids='process_data')
#     mysql = MySqlHook(mysql_conn_id='sql_cred')
#     for query, params in sqls:
#         mysql.run(query, parameters=params)

# execute_sql_task = PythonOperator(
#     task_id='execute_sql',
#     python_callable=execute_sql_queries,
#     dag=dag,
# )

# # ------------------ Task: Send Success Email ------------------
# def send_success_email_with_status(dag_run=None, **kwargs):
#     from airflow.utils.state import State

#     task_instances = dag_run.get_task_instances()
#     task_status_html = "<ul>"
#     for ti in task_instances:
#         color = 'green' if ti.state == State.SUCCESS else 'red'
#         task_status_html += f"<li><b>{ti.task_id}</b>: <span style='color:{color}'>{ti.state}</span></li>"
#     task_status_html += "</ul>"

#     html = f"""
#     <h3>Airflow DAG Completed Successfully ✅</h3>
#     <p>All task run summary:</p>
#     {task_status_html}
#     """

#     send_email_smtp(
#         subject="Workflow Success ✅",
#         html_content=html,
#         to_emails=SMTP_RECEIVER_EMAIL,
#         sender_email=SMTP_SENDER_EMAIL,
#         app_password=SMTP_APP_PASSWORD
#     )

# send_success_email_task = PythonOperator(
#     task_id='send_success_email',
#     python_callable=send_success_email_with_status,
#     dag=dag,
# )

# # ------------------ DAG Dependencies ------------------
# s3_sensor >> process_data_task >> execute_sql_task >> send_success_email_task



from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensor
import logging
import json
from helper.email_sender import send_email_smtp, send_failure_email_with_context

# ------------------ Logging Config ------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------ Load SMTP config ------------------
smtp_config_json = Variable.get("smtp_config")
smtp_config = json.loads(smtp_config_json)

SMTP_SENDER_EMAIL = smtp_config['sender_email']
SMTP_APP_PASSWORD = smtp_config['app_password']
SMTP_RECEIVER_EMAIL = smtp_config['receiver_emails']

# ------------------ Failure Callback ------------------
def failure_callback(context):
    send_failure_email_with_context(
        context=context,
        sender_email=SMTP_SENDER_EMAIL,
        app_password=SMTP_APP_PASSWORD,
        to_emails=SMTP_RECEIVER_EMAIL
    )

# ------------------ Default Args ------------------
def_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': failure_callback
}

# ------------------ DAG Definition ------------------
dag = DAG(
    's3_to_db_email_workflow_with_smtp',
    default_args=def_args,
    description='Pulls CSV from S3, inserts into MySQL, and sends SMTP emails',
    schedule=timedelta(days=1),
    catchup=False
)

# ------------------ Task 1: S3 Key Sensor ------------------
s3_sensor = S3KeySensor(
    task_id='s3_key_sensor',
    bucket_key='dv_prac/raw_data/*.csv',
    bucket_name='fug-de-training',
    aws_conn_id='aws_cred',
    wildcard_match=True,
    timeout=3600,
    poke_interval=60,
    dag=dag,
)

# ------------------ Task 2: Process Data from S3 ------------------
def process_data_and_generate_sql(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_cred')
    bucket = 'fug-de-training'
    prefix = 'dv_prac/raw_data/'

    file_keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
    logging.info(f"Files found: {file_keys}")

    if not file_keys:
        raise ValueError(f"No files found at {prefix}")

    key = file_keys[1]
    content = s3_hook.read_key(bucket_name=bucket, key=key)

    if not content.strip():
        raise ValueError(f"{key} is empty")

    lines = content.splitlines()
    headers = lines[0].split(',')
    sqls = []

    for line in lines[1:]:
        values = line.split(',')
        if len(values) != len(headers):
            logging.warning(f"Skipping: {line}")
            continue
        row = dict(zip(headers, values))
        sqls.append((
            """
            INSERT INTO dv_customers_new (
                customer_id, first_name, last_name, phone, email,
                street, city, state, zip_code, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (
                row['customer_id'],
                row['first_name'],
                row['last_name'],
                row['phone'] or None,
                row['email'],
                row['street'],
                row['city'],
                row['state'],
                row['zip_code']
            )
        ))

    return sqls

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data_and_generate_sql,
    dag=dag,
)

# ------------------ Task 3: Execute SQL ------------------
def execute_sql_queries(**kwargs):
    ti = kwargs['ti']
    sqls = ti.xcom_pull(task_ids='process_data')
    mysql = MySqlHook(mysql_conn_id='sql_cred')
    for query, params in sqls:
        mysql.run(query, parameters=params)

execute_sql_task = PythonOperator(
    task_id='execute_sql',
    python_callable=execute_sql_queries,
    dag=dag,
)

# ------------------ Task 4: Send Success Email ------------------
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.utils.session import provide_session

@provide_session
def send_success_email_with_status(dag_run=None, session=None, **kwargs):
    if dag_run is None:
        logging.warning("dag_run is None, cannot send success status email.")
        return

    task_instances = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_run.dag_id,
        TaskInstance.run_id == dag_run.run_id
    ).all()

    task_status_html = "<ul>"
    for ti in task_instances:
        color = "green" if ti.state == State.SUCCESS else "red"
        task_status_html += f"<li><b>{ti.task_id}</b>: <span style='color:{color}'>{ti.state}</span></li>"
    task_status_html += "</ul>"

    html = f"""
    <h3>✅ DAG Completed Successfully</h3>
    <p><strong>DAG ID:</strong> {dag_run.dag_id}<br>
    <strong>Run ID:</strong> {dag_run.run_id}<br>
    <strong>Execution Date:</strong> {dag_run.execution_date}</p>
    <p>Task Summary:</p>
    {task_status_html}
    """

    send_email_ses(
        subject=f"[Airflow Success] DAG {dag_run.dag_id}",
        html_content=html
    )

send_success_email_task = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email_with_status,
    # provide_context=True,
    dag=dag,
)

# ------------------ DAG Dependencies ------------------
s3_sensor >> process_data_task >> execute_sql_task >> send_success_email_task
