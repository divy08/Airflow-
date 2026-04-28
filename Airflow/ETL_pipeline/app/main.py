import os
import sys
# Go 2 levels up to get to ETL_pipeline/
current_file = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_file))
sys.path.insert(0, project_root)# import pandas as pd
import boto3
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from helper.AWS import AWS_S3Manager
from helper.DB import DatabaseManager
from helper.Kaggle_api import KaggleDataDownloader
from data_transformation.transformation import DataTransformer
from config.config import CONFIG

def main():
    try:
        # Initialize S3 Manager
        s3_manager = AWS_S3Manager(
            bucket_name=CONFIG["AWS"]["BUCKET_NAME"],
            aws_access_key_id=CONFIG["AWS"]["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=CONFIG["AWS"]["AWS_SECRET_ACCESS_KEY"],
            region_name=CONFIG["AWS"]["REGION_NAME"]
        )

        # Download folder from S3
        s3_manager.download_folder(
            s3_folder_key=CONFIG["AWS"]["FOLDER_KEY"]
        )

        # # Initialize Database Manager
        db_manager = DatabaseManager(
            host=CONFIG["DATABASE"]["HOST"],
            user=CONFIG["DATABASE"]["USER"],
            password=CONFIG["DATABASE"]["PASSWORD"],
            database=CONFIG["DATABASE"]["DATABASE"]
        )
        
        output_path = os.path.join(CONFIG["LOCAL_PATHS"]["INPUT_FOLDER"], "House_Price_DB.csv")
        db_manager.export_table_to_csv(
            table_name="dv_house_price",
            csv_file_path=output_path
            
        )

        kaggle_data_downloader = KaggleDataDownloader(
            dataset=CONFIG["KAGGLE"]["KAGGLE_API_PATH"],
            file_name="House_Price_Kaggle.csv"
        )
        kaggle_data_downloader.download()


        # Data Transformation
        input_folder = CONFIG["LOCAL_PATHS"]["INPUT_FOLDER"]
        output_folder = CONFIG["LOCAL_PATHS"]["OUTPUT_FOLDER"]

        if os.path.exists(input_folder):
            transformer = DataTransformer(input_folder, output_folder)
            transformer.run()
            transformer.clean_downloads_folder()
        else:
            print(f"Input folder '{input_folder}' not found for data transformation.")


        # Zip and upload the merged CSV
        transformer.zip_and_remove_file(CONFIG["LOCAL_PATHS"]["TRANSFORMED_FILE"])

        print("ETL pipeline completed successfully.")


        s3_manager.upload_zip_with_timestamp(
            file_path=CONFIG["LOCAL_PATHS"]["ZIP_FILE"],
            s3_folder_key=CONFIG["AWS"]["FOLDER_ZIP_KEY"]
        )


    except Exception as e:
        print(f"An error occurred: {e}")

    

if __name__ == "__main__":
    main()



