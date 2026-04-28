import os
import sys
import boto3


class AWS_S3Manager:

    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    # def upload_file(self, local_filename, s3_key):
        
    #     '''
    #     arguments: local_filename, s3_key
        
    #     Upload file to S3_bucket 
    #     ''' 
    #     try:

    #         self.s3_client.upload_file(local_filename, self.bucket_name, s3_key)
    #         print(f"File '{local_filename}' uploaded to S3 bucket as '{s3_key}'.")

    #     except Exception as e:
    #         print()

    

    # def upload_folder(self, local_folder_path, s3_folder_key):
    #     '''
    #     arguments: 
    #         local_folder_path: Path to the local folder
    #         s3_folder_key: Existing folder (prefix) in S3

    #     Uploads all files from the local folder directly into the existing S3 folder.
    #     '''
    #     try:
    #         for root, dirs, files in os.walk(local_folder_path):
    #             for file in files:
    #                 local_file_path = os.path.join(root, file)
                    
    #                 # Only use the filename (not folder structure)
    #                 s3_key = os.path.join(s3_folder_key, file).replace("\\", "/")

    #                 self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
    #                 print(f"Uploaded '{local_file_path}' to 's3://{self.bucket_name}/{s3_key}'.")

    #     except Exception as e:
    #         print(f"Error uploading folder: {e}")


    def download_file(self, s3_key, local_filename, target_directory=r"raw\downloads"):
        
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
        
    

    def download_folder(self, s3_folder_key, target_directory=r"raw\downloads"):
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

    

