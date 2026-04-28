import logging
import tempfile
import zipfile
import os
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from config import S3_BUCKET, S3_PREFIX

def archive_source_file(**kwargs):
    """Archive source file by moving it to prep_data folder in zip format"""
    try:
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_cred')
        
        # Get the source filename from previous task
        source_file = ti.xcom_pull(task_ids='extract_data_from_s3', key='source_filename')
        if not source_file:
            logging.warning("No source file found to archive")
            return False
        
        # Generate timestamp and new filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(source_file))[0]
        zip_filename = f"{base_name}_{timestamp}.zip"
        prep_path = f"dv_prac/prep_data/{zip_filename}"
        
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file_path = os.path.join(tmp_dir, os.path.basename(source_file))
            local_zip_path = os.path.join(tmp_dir, zip_filename)
            
            try:
                # Download the file from S3
                logging.info(f"Downloading {source_file} from S3")
                file_content = s3_hook.read_key(
                    key=source_file,
                    bucket_name=S3_BUCKET
                )
                
                # Write content to local file
                with open(local_file_path, 'w') as f:
                    f.write(file_content)
                
                # Create zip file with the actual content
                logging.info(f"Creating ZIP archive at {local_zip_path}")
                with zipfile.ZipFile(local_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(
                        local_file_path,
                        arcname=os.path.basename(source_file)
                    )
                
                # Verify the ZIP contains our file
                with zipfile.ZipFile(local_zip_path, 'r') as zipf:
                    file_list = zipf.namelist()
                    if os.path.basename(source_file) not in file_list:
                        raise AirflowException("Failed to add file to ZIP archive")
                    file_size = zipf.getinfo(os.path.basename(source_file)).file_size
                    if file_size == 0:
                        raise AirflowException("ZIP contains empty file")
                
                # Upload the zip file to S3
                logging.info(f"Uploading ZIP to {prep_path}")
                s3_hook.load_file(
                    filename=local_zip_path,
                    key=prep_path,
                    bucket_name=S3_BUCKET,
                    replace=True
                )
                
                # Delete the original file
                logging.info(f"Deleting original file {source_file}")
                s3_hook.delete_objects(bucket=S3_BUCKET, keys=source_file)
                
                logging.info(f"Successfully moved {source_file} to {prep_path}")
                return True
                
            except Exception as e:
                logging.error(f"Error during file processing: {str(e)}")
                raise AirflowException(f"File processing failed: {str(e)}")
                
    except Exception as e:
        logging.error(f"File archiving failed: {str(e)}")
        raise AirflowException(f"File archiving failed: {str(e)}")