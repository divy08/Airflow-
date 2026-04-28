from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import json
import urllib.parse

# Path to the .kaggle directory
KAGGLE_DIR = "/home/airflow/.kaggle"  # or "/root/.kaggle" based on your Airflow user

def create_kaggle_json():
    creds = Variable.get("kaggle_creds", deserialize_json=True)

    os.makedirs(KAGGLE_DIR, exist_ok=True)

    kaggle_path = os.path.join(KAGGLE_DIR, "kaggle.json")
    with open(kaggle_path, "w") as f:
        json.dump({
            "username": creds["username"],
            "key": creds["key"]
        }, f)

    os.chmod(kaggle_path, 0o600)  # Ensure correct file permissions

def download_kaggle_data():
    import os
    from kaggle.api.kaggle_api_extended import KaggleApi

    download_path = "/home/airflow/plugins/kaggle_data"

    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files("divyvishwakarma/house-price-divy", path=download_path, unzip=True)

        if os.path.exists(download_path):
            files = os.listdir(download_path)
            if files:
                print(f"Dataset downloaded to: {download_path}")
                print("Downloaded files:")
                for file in files:
                    print(f" - {file}")
            else:
                raise Exception("Download folder exists but is empty.")
        else:
            raise Exception(f"Download path does not exist: {download_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def insert_into_mysql():


    # connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


    # # SQLAlchemy connection string
    cred = Variable.get("sql_creds", deserialize_json=True)
    password = urllib.parse.quote_plus(cred['password'])
    engine = create_engine(
        f"mysql+pymysql://{cred['login']}:{password}@{cred['host']}:{cred['port']}/{cred['schema']}"
        # f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"        

    )   
    data_path = "/home/airflow/plugins/kaggle_data"


    # Example: assuming a CSV file exists in the dataset
    csv_file = os.path.join(data_path, "House_Price_Kaggle.csv")  # replace with actual file name

    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        df.to_sql(name="dv_house_prices_new", con=engine, if_exists="append", index=False)
        print("Data successfully inserted into MySQL.")
    else:
        raise FileNotFoundError(f"CSV file not found at: {csv_file}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='kaggle_dataset_downloader',
    default_args=default_args,
    description='Download Kaggle dataset using kaggle.json generated from Airflow Variable',
    start_date=datetime(2025, 5, 8),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_kaggle_json_task = PythonOperator(
        task_id='create_kaggle_json',
        python_callable=create_kaggle_json,
    )

    download_data_task = PythonOperator(
        task_id='download_kaggle_data',
        python_callable=download_kaggle_data,
    )

    insert_into_mysql_task = PythonOperator(
        task_id='insert_into_mysql',
        python_callable=insert_into_mysql,
    )




    create_kaggle_json_task >> download_data_task >> insert_into_mysql_task