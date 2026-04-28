import os
import sys
import pandas as pd
import boto3
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from helper.s3_manager import AWS_S3Manager
from helper.sql_manager import DatabaseManager
from config.config import CONFIG
# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_transformation.transformation import DataTransformer


def main():
    try:
        # Initialize S3 Manager
        s3_manager = AWS_S3Manager(
            bucket_name=CONFIG["AWS"]["BUCKET_NAME"],
            aws_access_key_id=CONFIG["AWS"]["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=CONFIG["AWS"]["AWS_SECRET_ACCESS_KEY"],
            region_name=CONFIG["AWS"]["REGION_NAME"]
        )

        # Upload local folder to S3
        local_upload_folder = CONFIG["LOCAL_PATHS"]["LOCAL_UPLOAD_FOLDER"]
        if os.path.exists(local_upload_folder):
            s3_manager.upload_folder(
                local_folder_path=local_upload_folder,
                s3_folder_key=CONFIG["AWS"]["FOLDER_KEY"]
            )
        else:
            print(f"Local upload folder '{local_upload_folder}' not found.")

        # Download folder from S3
        s3_manager.download_folder(
            s3_folder_key=CONFIG["AWS"]["FOLDER_KEY"]
        )

        # Data Transformation
        input_folder = CONFIG["LOCAL_PATHS"]["INPUT_FOLDER"]
        output_folder = CONFIG["LOCAL_PATHS"]["OUTPUT_FOLDER"]

        if os.path.exists(input_folder):
            transformer = DataTransformer(input_folder, output_folder)
            transformer.run()
            transformer.clean_downloads_folder()
        else:
            print(f"Input folder '{input_folder}' not found for data transformation.")

        # Initialize Database Manager
        db_manager = DatabaseManager(
            host=CONFIG["DATABASE"]["HOST"],
            user=CONFIG["DATABASE"]["USER"],
            password=CONFIG["DATABASE"]["PASSWORD"],
            database=CONFIG["DATABASE"]["DATABASE"]
        )

        # Import transformed CSV into Database
        transformed_file = CONFIG["LOCAL_PATHS"]["TRANSFORMED_FILE"]
        if os.path.exists(transformed_file):
            db_manager.import_csv_to_table(
                file_path=transformed_file,
                table_name="dv_merge"
            )
        else:
            print(f"Transformed CSV file '{transformed_file}' not found.")

        # Zip and upload the merged CSV
        s3_manager.zip_and_remove_file(transformed_file)

        s3_manager.upload_zip_with_timestamp(
            file_path=CONFIG["LOCAL_PATHS"]["ZIP_FILE"],
            s3_folder_key=CONFIG["AWS"]["FOLDER_ZIP_KEY"]
        )

        # Delete raw data from S3
        s3_manager.delete_data(
            source_folder=CONFIG["AWS"]["FOLDER_KEY"]
        )

        print("ETL pipeline completed successfully.")


    except Exception as e:
        print(f"An error occurred: {e}")

    os.remove(CONFIG["LOCAL_PATHS"]["ZIP_FILE"])
    print("Finished")


if __name__ == "__main__":
    main()
