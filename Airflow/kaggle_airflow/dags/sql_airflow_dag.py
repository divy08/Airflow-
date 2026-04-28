from airflow.models import DAG, Variable, Connection
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
}



with DAG('sql_task', default_args=default_args, schedule_interval=None) as dag:
    # Define the task
    sql_task = MySqlOperator(
        task_id='sql_task',
        mysql_conn_id='sql_cred',
        sql=open('/opt/airflow/plugins/sql/insert.sql').read(),  # load absolute path
        # template_ext=[],  # Disable Jinja templating
    )
    

sql_task
