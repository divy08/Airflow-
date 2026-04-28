import os
import boto3
from datetime import datetime
import zipfile


class AWS_S3Manager:

    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    def upload_file(self, local_filename, s3_key):
        
        '''
        arguments: local_filename, s3_key
        
        Upload file to S3_bucket 
        ''' 
        try:

            self.s3_client.upload_file(local_filename, self.bucket_name, s3_key)
            print(f"File '{local_filename}' uploaded to S3 bucket as '{s3_key}'.")

        except Exception as e:
            print()

            
    def download_file(self, s3_key, local_filename, target_directory="ETL_pipeline/raw_data"):
        
        '''
        arguments: local_filename, target_directory,s3_key
        
        Downloading the file to the target directory specified   in the S3_bucket configuration file and 
        rename the file to the local_filename specified in the S3_bucket configuration file.
        '''
        try:

            if not os.path.exists(target_directory):
                os.makedirs(target_directory)# create the target directory if it doesn't exist

            # Construct the full local path
            local_path = os.path.join(target_directory, local_filename)

            # Download the file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"File '{s3_key}' downloaded from S3 bucket and saved as '{local_path}'.")

        except Exception as e:
            raise e
        
    

    def download_folder(self, s3_folder_key, target_directory="raw_data"):
        '''
        arguments: 
            s3_folder_key: S3 "folder" (prefix) to download from
            target_directory: Local directory to save the files

        Downloads all files under the specified S3 folder (prefix) into the target local directory.
        '''

        if not os.path.exists(target_directory):
            os.makedirs(target_directory)

        # List all objects under the specified S3 prefix
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_folder_key)

        if 'Contents' not in response:
            print(f"No files found in S3 folder '{s3_folder_key}'.")
            return

        for obj in response['Contents']:
            s3_key = obj['Key']
            
            # Skip if the object is a "folder" (i.e., ends with '/')
            if s3_key.endswith('/'):
                continue

            # Calculate relative path after s3_folder_key
            relative_path = os.path.relpath(s3_key, s3_folder_key)

            # Construct local file path
            local_file_path = os.path.join(target_directory, relative_path)

            # Make sure the local folder exists
            local_folder = os.path.dirname(local_file_path)
            if not os.path.exists(local_folder):
                os.makedirs(local_folder)

            # Download the file
            self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
            print(f"Downloaded '{s3_key}' to '{local_file_path}'.")

    


    def zip_and_remove_file(self, file_path):
        """
        Compress the given file into a zip archive and delete the original file.
        
        Args:
            file_path (str): Full path to the file to be zipped and deleted.
        
        Returns:
            zip_file_path (str): Path to the created zip file.
        """
        try:
            # Create the ZIP file path (same name, .zip extension)
            zip_file_path = file_path.replace('.csv', '.zip')

            # Create ZIP file
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, arcname=os.path.basename(file_path))

            print(f"File zipped successfully: {zip_file_path}")

            # Remove the original CSV
            os.remove(file_path)
            print(f"Original file deleted: {file_path}")

            return zip_file_path
        

        except Exception as e:
            print(f"An error occurred during zipping/removing file: {e}")
            raise e
    def upload_zip_with_timestamp(self, file_path, s3_folder_key):
        """
        Zip the file, remove the original, and upload the zip to S3 with timestamp.

        Args:
            file_path (str): Local path of the file to zip and upload.
            s3_folder_key (str): S3 folder key where zip will be uploaded.
        """
        

        base_name = os.path.basename(file_path)
        name, ext = os.path.splitext(base_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_name = f"{name}_{timestamp}{ext}"

        # 4. Full S3 Key
        s3_key = os.path.join(s3_folder_key, timestamped_name)

        # 5. Upload to S3
        self.upload_file(local_filename=file_path, s3_key=s3_key)
        print(f"Uploaded ZIP with timestamp to S3: {s3_key}")
 