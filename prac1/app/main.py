import os
import sys
import boto3
from helper.s3_manager import AWS_S3Manager
from config.config import CONFIG


# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def main():
    try:
        # Initialize S3 Manager
        s3_manager = AWS_S3Manager(
            bucket_name=CONFIG["AWS"]["BUCKET_NAME"],
            aws_access_key_id=CONFIG["AWS"]["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=CONFIG["AWS"]["AWS_SECRET_ACCESS_KEY"],
            region_name=CONFIG["AWS"]["REGION_NAME"]
        )
        print(s3_manager.__getattribute__)

        # Download folder from S3
        s3_manager.download_folder(
            s3_folder_key="dv-prac/dv_raw/"
        )

        s3_manager.upload_file(
            local_filename=r"raw\downloads\brands.csv",
            s3_key="dv-prac/dv_raw/")
        

        s3_manager.download_file(
            local_filename="raw\downloads\archive",
            target_directory="raw\downloads"
        )
        
        
        # Upload local folder to S3
        local_upload_folder = r"raw\downloads\archive"
        s3_manager.upload_(
                local_folder_path=local_upload_folder,
                s3_folder_key="dv-prac/dv_raw/"
            )
        


    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
