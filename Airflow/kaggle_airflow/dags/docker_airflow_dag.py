from airflow.models import DAG, Variable
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import json

# # Get config variable and convert to environment-style flat dict
# creds = Variable.get("Config", deserialize_json=True)

# # Flatten the nested dict to environment variables
# env_vars = {
#     "AWS_BUCKET_NAME": creds["AWS"]["BUCKET_NAME"],
#     "AWS_ACCESS_KEY_ID": creds["AWS"]["AWS_ACCESS_KEY_ID"],
#     "AWS_SECRET_ACCESS_KEY": creds["AWS"]["AWS_SECRET_ACCESS_KEY"],
#     "REGION_NAME": creds["AWS"]["REGION_NAME"],
#     "FOLDER_KEY": creds["AWS"]["FOLDER_KEY"],
#     "FOLDER_ZIP_KEY": creds["AWS"]["FOLDER_ZIP_KEY"],
#     "KAGGLE_LOCAL_DOWNLOAD_FOLDER": creds["KAGGLE"]["LOCAL_DOWNLOAD_FOLDER"],
#     "KAGGLE_API_PATH": creds["KAGGLE"]["KAGGLE_API_PATH"],
#     "INPUT_FOLDER": creds["LOCAL_PATHS"]["INPUT_FOLDER"],
#     "OUTPUT_FOLDER": creds["LOCAL_PATHS"]["OUTPUT_FOLDER"],
#     "TRANSFORMED_FILE": creds["LOCAL_PATHS"]["TRANSFORMED_FILE"],
#     "ZIP_FILE": creds["LOCAL_PATHS"]["ZIP_FILE"],
#     "DB_HOST": creds["DATABASE"]["HOST"],
#     "DB_USER": creds["DATABASE"]["USER"],
#     "DB_PASSWORD": creds["DATABASE"]["PASSWORD"],
#     "DB_DATABASE": creds["DATABASE"]["DATABASE"]
# }



creds_json = json.dumps(Variable.get("Config", deserialize_json=True))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    
}

with DAG('docker_image_task', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    run_docker = DockerOperator(
        task_id='run_docker_container',
        image='kaggel_new',
        command='python /src/app/main.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment= {"cred": creds_json},
        mount_tmp_dir=False,  # avoid temp volume mounting issue
    )

run_docker