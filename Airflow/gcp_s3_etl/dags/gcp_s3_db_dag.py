# # from datetime import datetime, timedelta
# # from airflow import DAG
# # from airflow.models import Variable
# # from airflow.decorators import task, task_group
# # from airflow.models.dataset import Dataset
# # from airflow.providers.mysql.operators.mysql import MySqlOperator
# # from airflow.providers.mysql.hooks.mysql import MySqlHook
# # from airflow.hooks.base import BaseHook
# # import boto3
# # from botocore.exceptions import ClientError
# # from google.oauth2 import service_account
# # from googleapiclient.discovery import build
# # from googleapiclient.http import MediaIoBaseDownload
# # import io
# # import os
# # import zipfile
# # import pandas as pd
# # import logging
# # import re
# # import time
# # import json
# # from airflow.exceptions import AirflowException

# # # Configure logging
# # logger = logging.getLogger(__name__)
# # logging.basicConfig(level=logging.INFO)

# # # Define datasets for triggering
# # LANDING_ZONE_DS = Dataset("file://landing_zone")
# # ARCHIVE_DS = Dataset("file://archive")

# # default_args = {
# #     'owner': 'data_team',
# #     'depends_on_past': False,
# #     'start_date': datetime(2023, 1, 1),
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=1),
# #     'email_on_failure': True,
# #     'email_on_retry': True,
# # }

# # CONFIG = {
# #     # Local paths
# #     'landing_path': "/gcp/landing_folder",
# #     'archive_path': "/gcp/archive_folder",
# #     'temp_path': "/data/temp",
    
# #     # Database connection
# #     'mysql_conn_id': "mysql_conn",
    
# #     # S3 connection
# #     's3_conn_id': "aws_s3_conn",
# #     's3_bucket': "fug-de-training/dv_prac",
    
# #     # Google Drive folder structure
# #     'gdrive_landing_root': '1Hn74Ia7cU8LJmMAiJlYreptEPjoP-MsG',
# #     'gdrive_archive_root': '1KkXT7jhpbO-MaucHw6lhWtWdlBZsf9TK',
    
# #     # File formats configuration - VERIFIED CORRECT FOLDER IDs
# #     'file_formats': {
# #         'csv': {
# #             'folder': 'csv_data',
# #             'extensions': ['.csv'],
# #             'landing_folder_id': '13ZV6C8tT1P2sG8uu2bg19w3uI-aCSEIb',
# #             'archive_folder_id': '1z00QnPZJMeR1Kuz1Ba9f6gobT8YLOUni'
# #         },
# #         'json': {
# #             'folder': 'json_data',
# #             'extensions': ['.json'],
# #             'landing_folder_id': '1yd3gaLu46SKrdZMWERWeNIsUua0yJQJ3',
# #             'archive_folder_id': '1oJ_H5pTvHcMMuXREE_TxbRGjgu7CTXGD'
# #         },
# #         'excel': {
# #             'folder': 'excel_data',
# #             'extensions': ['.xlsx', '.xls'],
# #             'landing_folder_id': '1HP_Gtfk0S2iA_nh5ZPa7NLh6-Bt8HdZ2',
# #             'archive_folder_id': '1-mAg1EukJnEuL3QmZ4LSSuIha5PfWzDZ'
# #         }
# #     },
    
# #     # Processing config
# #     'chunk_size': 10000
# # }

# # # Debug: Print folder mappings at startup
# # logger.info("=== Starting DAG with these folder mappings ===")
# # for fmt, config in CONFIG['file_formats'].items():
# #     logger.info(f"{fmt.upper()} Configuration:")
# #     logger.info(f"  Landing Folder ID: {config['landing_folder_id']}")
# #     logger.info(f"  Archive Folder ID: {config['archive_folder_id']}")
# #     logger.info(f"  Valid Extensions: {config['extensions']}")
# # logger.info("==============================================")

# # def get_gdrive_service():
# #     """Create authenticated Google Drive service using Airflow Variable"""
# #     try:
# #         creds = Variable.get("gcp_config", deserialize_json=True)
# #         if not creds:
# #             raise AirflowException("GCP configuration not found in Airflow Variables")
        
# #         required_fields = ['type', 'project_id', 'private_key_id', 'private_key',
# #                          'client_email', 'client_id', 'auth_uri', 'token_uri',
# #                          'auth_provider_x509_cert_url', 'client_x509_cert_url']
        
# #         missing_fields = [f for f in required_fields if f not in creds]
# #         if missing_fields:
# #             raise AirflowException(f"Missing required fields in GCP config: {missing_fields}")
            
# #         if 'private_key' in creds and isinstance(creds['private_key'], str):
# #             creds['private_key'] = creds['private_key'].replace('\\n', '\n')
        
# #         credentials = service_account.Credentials.from_service_account_info(
# #             creds,
# #             scopes=['https://www.googleapis.com/auth/drive']
# #         )
        
# #         return build('drive', 'v3', credentials=credentials)
# #     except Exception as e:
# #         logger.error(f"Google Drive authentication failed: {str(e)}")
# #         raise AirflowException(f"Google Drive service creation failed: {str(e)}")

# # def list_gdrive_files(folder_id, expected_extensions):
# #     """List files in Google Drive folder with strict extension checking"""
# #     try:
# #         service = get_gdrive_service()
# #         results = service.files().list(
# #             q=f"'{folder_id}' in parents and trashed=false",
# #             pageSize=100,
# #             fields="files(id, name, mimeType, fileExtension)"
# #         ).execute()
        
# #         files = results.get('files', [])
# #         filtered_files = []
        
# #         for file in files:
# #             # Get extension from name or fileExtension property
# #             file_ext = os.path.splitext(file['name'])[1].lower()
# #             if not file_ext and 'fileExtension' in file:
# #                 file_ext = f".{file['fileExtension'].lower()}"
            
# #             # Check if extension matches expected
# #             if any(file_ext == ext.lower() for ext in expected_extensions):
# #                 filtered_files.append(file)
# #             else:
# #                 logger.warning(f"Skipping file {file['name']} with extension {file_ext} - expected {expected_extensions}")
        
# #         return filtered_files
# #     except Exception as e:
# #         logger.error(f"Failed to list files in folder {folder_id}: {str(e)}")
# #         raise AirflowException(f"Google Drive listing error: {str(e)}")

# # def move_gdrive_file(file_id, target_folder_id):
# #     """Move file to target folder in Google Drive"""
# #     try:
# #         service = get_gdrive_service()
        
# #         # Get current parent folders
# #         file = service.files().get(
# #             fileId=file_id,
# #             fields='parents'
# #         ).execute()
        
# #         previous_parents = ",".join(file.get('parents', []))
        
# #         # Move the file
# #         updated_file = service.files().update(
# #             fileId=file_id,
# #             addParents=target_folder_id,
# #             removeParents=previous_parents,
# #             fields='id, parents'
# #         ).execute()
        
# #         return updated_file
# #     except Exception as e:
# #         logger.error(f"Failed to move file {file_id}: {str(e)}")
# #         raise AirflowException(f"Google Drive file move failed: {str(e)}")

# # def read_gdrive_file(file_id, file_name, file_format):
# #     """Read file content directly from Google Drive"""
# #     try:
# #         service = get_gdrive_service()
# #         request = service.files().get_media(fileId=file_id)
        
# #         # Use a buffer to hold the file content
# #         file_buffer = io.BytesIO()
# #         downloader = MediaIoBaseDownload(file_buffer, request)
        
# #         done = False
# #         while not done:
# #             status, done = downloader.next_chunk()
# #             logger.info(f"Downloaded {int(status.progress() * 100)}% of {file_name}")
        
# #         file_buffer.seek(0)  # Rewind the buffer
        
# #         # Read based on file format
# #         if file_format == 'csv':
# #             df = pd.read_csv(file_buffer)
# #         elif file_format == 'json':
# #             df = pd.read_json(file_buffer)
# #         elif file_format == 'excel':
# #             df = pd.read_excel(file_buffer)
# #         else:
# #             raise ValueError(f"Unsupported format: {file_format}")
            
# #         return df
# #     except Exception as e:
# #         logger.error(f"Failed to read file {file_name}: {str(e)}")
# #         raise AirflowException(f"File reading error: {str(e)}")

# # def validate_employee_data(df):
# #     """Validate employee data structure"""
# #     try:
# #         # Validate employee_id (4-digit)
# #         df['employee_id'] = df['employee_id'].astype(str)
# #         invalid_ids = df[~df['employee_id'].str.match(r'^\d{4}$')]
# #         if not invalid_ids.empty:
# #             raise ValueError(f"Invalid employee IDs: {invalid_ids['employee_id'].tolist()}")
            
# #         # Validate name fields
# #         df['firstname'] = df['firstname'].str.strip()
# #         df['lastname'] = df['lastname'].str.strip()
        
# #         name_length_issues = df[
# #             (df['firstname'].str.len() > 20) | 
# #             (df['lastname'].str.len() > 20)
# #         ]
# #         if not name_length_issues.empty:
# #             raise ValueError(f"Name length exceeds 20 chars for records: {name_length_issues.index.tolist()}")
        
# #         # Validate email format
# #         email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
# #         invalid_emails = df[~df['email'].str.match(email_regex, na=False)]
# #         if not invalid_emails.empty:
# #             raise ValueError(f"Invalid email formats: {invalid_emails['email'].tolist()}")
        
# #         # Validate salary
# #         try:
# #             df['salary'] = pd.to_numeric(df['salary'])
# #         except ValueError as e:
# #             raise ValueError(f"Invalid salary values: {str(e)}")
        
# #         return df
# #     except Exception as e:
# #         logger.error(f"Data validation failed: {str(e)}")
# #         raise

# # def process_employee_file(file_id, file_name, file_format):
# #     """Process employee file with validation"""
# #     try:
# #         # Read and validate file
# #         df = read_gdrive_file(file_id, file_name, file_format)
            
# #         # Validate required columns
# #         required_cols = {'employee_id', 'firstname', 'lastname', 'salary', 'email'}
# #         if not required_cols.issubset(df.columns):
# #             missing = required_cols - set(df.columns)
# #             raise ValueError(f"Missing columns: {missing}")
            
# #         return validate_employee_data(df).to_dict('records')
# #     except Exception as e:
# #         logger.error(f"Failed to process {file_name}: {str(e)}")
# #         raise AirflowException(f"File processing error: {str(e)}")

# # def load_to_staging_table(conn_id, data, file_info):
# #     """Load data to staging table"""
# #     hook = MySqlHook(mysql_conn_id=conn_id)
# #     conn = hook.get_conn()
# #     cursor = conn.cursor()
    
# #     try:
# #         # Prepare batch insert
# #         values = []
# #         for record in data:
# #             values.append((
# #                 record['employee_id'],
# #                 record['firstname'],
# #                 record['lastname'],
# #                 float(record['salary']),
# #                 record['email'],
# #                 file_info['source_system'],
# #                 file_info['file_name']
# #             ))
        
# #         # Batch insert
# #         cursor.executemany("""
# #             INSERT INTO dv_stg_employees 
# #             (employee_id, firstname, lastname, salary, email, source_system, file_name)
# #             VALUES (%s, %s, %s, %s, %s, %s, %s)
# #         """, values)
        
# #         # Update file status
# #         cursor.execute("""
# #             UPDATE dv_stg_files 
# #             SET status = 'staged', processed_at = CURRENT_TIMESTAMP 
# #             WHERE id = %s
# #         """, (file_info['file_id'],))
        
# #         conn.commit()
# #         logger.info(f"Staged {len(data)} records from {file_info['file_name']}")
# #     except Exception as e:
# #         conn.rollback()
# #         logger.error(f"Staging failed: {str(e)}")
        
# #         # Update error status
# #         hook.run("""
# #             UPDATE dv_stg_files 
# #             SET status = 'failed', error_message = %s 
# #             WHERE id = %s
# #         """, parameters=(str(e)[:500], file_info['file_id']))
        
# #         raise AirflowException(f"Staging error: {str(e)}")
# #     finally:
# #         cursor.close()
# #         conn.close()

# # def process_staging_to_main(conn_id, file_info):
# #     """Process data from staging to main table"""
# #     hook = MySqlHook(mysql_conn_id=conn_id)
# #     conn = hook.get_conn()
# #     cursor = conn.cursor()
    
# #     try:
# #         # Get count of records to process
# #         cursor.execute("""
# #             SELECT COUNT(*) 
# #             FROM dv_stg_employees 
# #             WHERE source_system = %s AND file_name = %s
# #         """, (file_info['source_system'], file_info['file_name']))
# #         record_count = cursor.fetchone()[0]
        
# #         if record_count == 0:
# #             logger.warning(f"No records found for {file_info['file_name']}")
# #             return
        
# #         # Transfer to main table
# #         cursor.execute("""
# #             INSERT INTO dv_employees (employee_id, firstname, lastname, salary, email)
# #             SELECT employee_id, firstname, lastname, salary, email
# #             FROM dv_stg_employees
# #             WHERE source_system = %s AND file_name = %s
# #             ON DUPLICATE KEY UPDATE
# #                 firstname = VALUES(firstname),
# #                 lastname = VALUES(lastname),
# #                 salary = VALUES(salary),
# #                 email = VALUES(email)
# #         """, (file_info['source_system'], file_info['file_name']))
        
# #         # Log success
# #         cursor.execute("""
# #             INSERT INTO dv_employee_load_audit 
# #             (file_name, source_system, records_processed, status)
# #             VALUES (%s, %s, %s, 'processed')
# #         """, (file_info['file_name'], file_info['source_system'], record_count))
        
# #         # Cleanup staging
# #         cursor.execute("""
# #             DELETE FROM dv_stg_employees
# #             WHERE source_system = %s AND file_name = %s
# #         """, (file_info['source_system'], file_info['file_name']))
        
# #         # Update file status
# #         cursor.execute("""
# #             UPDATE dv_stg_files 
# #             SET status = 'processed', processed_at = CURRENT_TIMESTAMP 
# #             WHERE id = %s
# #         """, (file_info['file_id'],))
        
# #         conn.commit()
# #         logger.info(f"Processed {record_count} records from {file_info['file_name']}")
# #     except Exception as e:
# #         conn.rollback()
# #         logger.error(f"Processing failed: {str(e)}")
        
# #         # Update error status
# #         hook.run("""
# #             UPDATE dv_stg_files 
# #             SET status = 'failed', error_message = %s 
# #             WHERE id = %s
# #         """, parameters=(str(e)[:500], file_info['file_id']))
        
# #         raise AirflowException(f"Processing error: {str(e)}")
# #     finally:
# #         cursor.close()
# #         conn.close()

# # def create_s3_client():
# #     """Create authenticated S3 client"""
# #     try:
# #         conn = BaseHook.get_connection(CONFIG['s3_conn_id'])
# #         return boto3.client(
# #             's3',
# #             aws_access_key_id=conn.login,
# #             aws_secret_access_key=conn.password,
# #             region_name=conn.extra_dejson.get('region_name', 'us-west-2')
# #         )
# #     except Exception as e:
# #         logger.error(f"S3 client creation failed: {str(e)}")
# #         raise AirflowException(f"S3 setup failed: {str(e)}")

# # def create_archive(source_path, archive_path, file_list):
# #     """Create zip archive of processed files"""
# #     try:
# #         os.makedirs(os.path.dirname(archive_path), exist_ok=True)
# #         zip_path = f"{archive_path}.zip"
        
# #         with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
# #             for file in file_list:
# #                 file_path = os.path.join(source_path, file)
# #                 if os.path.exists(file_path):
# #                     zipf.write(file_path, arcname=file)
# #                 else:
# #                     logger.warning(f"File not found: {file_path}")
        
# #         return zip_path
# #     except Exception as e:
# #         logger.error(f"Archive creation failed: {str(e)}")
# #         if os.path.exists(zip_path):
# #             os.remove(zip_path)
# #         raise AirflowException(f"Archive error: {str(e)}")

# # with DAG(
# #     'employee_data_processing',
# #     default_args=default_args,
# #     schedule=[LANDING_ZONE_DS],
# #     catchup=False,
# #     max_active_runs=1,
# #     tags=['employee_data', 'mysql', 's3', 'gdrive'],
# #     description='Process employee data from S3 and Google Drive to MySQL',
# # ) as dag:

# #     create_tables = MySqlOperator(
# #         task_id='create_tables',
# #         mysql_conn_id=CONFIG['mysql_conn_id'],
# #         sql="""
# #         CREATE TABLE IF NOT EXISTS dv_employees (
# #             employee_id INT(4) ZEROFILL PRIMARY KEY,
# #             firstname VARCHAR(20) NOT NULL,
# #             lastname VARCHAR(20) NOT NULL,
# #             salary DECIMAL(10,2) NOT NULL,
# #             email VARCHAR(100) NOT NULL UNIQUE,
# #             processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# #         );
        
# #         CREATE TABLE IF NOT EXISTS dv_stg_employees (
# #             id INT AUTO_INCREMENT PRIMARY KEY,
# #             employee_id VARCHAR(4) NOT NULL,
# #             firstname VARCHAR(20) NOT NULL,
# #             lastname VARCHAR(20) NOT NULL,
# #             salary DECIMAL(10,2) NOT NULL,
# #             email VARCHAR(100) NOT NULL,
# #             source_system VARCHAR(50) NOT NULL,
# #             file_name VARCHAR(255) NOT NULL,
# #             staged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# #             INDEX idx_source_file (source_system, file_name)
# #         );
        
# #         CREATE TABLE IF NOT EXISTS dv_stg_files (
# #             id INT AUTO_INCREMENT PRIMARY KEY,
# #             source_system VARCHAR(50),
# #             file_format VARCHAR(10),
# #             file_name VARCHAR(255),
# #             file_path VARCHAR(512),
# #             status VARCHAR(20) DEFAULT 'pending',
# #             error_message TEXT,
# #             processed_at TIMESTAMP NULL,
# #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# #         );
        
# #         CREATE TABLE IF NOT EXISTS dv_employee_load_audit (
# #             id INT AUTO_INCREMENT PRIMARY KEY,
# #             file_name VARCHAR(255) NOT NULL,
# #             source_system VARCHAR(50) NOT NULL,
# #             records_processed INT NOT NULL,
# #             status VARCHAR(20) NOT NULL,
# #             error_message TEXT,
# #             processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# #         );
# #         """
# #     )

# #     @task_group(group_id='s3_processing')
# #     def process_s3():
# #         for file_format, format_config in CONFIG['file_formats'].items():
# #             @task_group(group_id=f's3_{file_format}')
# #             def process_s3_format():
# #                 @task(task_id=f'list_files')
# #                 def list_files():
# #                     try:
# #                         s3 = create_s3_client()
# #                         response = s3.list_objects_v2(
# #                             Bucket=CONFIG['s3_bucket'],
# #                             Prefix=f"{format_config['folder']}/"
# #                         )
# #                         files = [
# #                             obj['Key'].split('/')[-1] 
# #                             for obj in response.get('Contents', []) 
# #                             if any(obj['Key'].lower().endswith(ext) for ext in format_config['extensions'])
# #                         ]
# #                         logger.info(f"Found {len(files)} {file_format} files in S3")
# #                         return files
# #                     except Exception as e:
# #                         logger.error(f"S3 listing failed: {str(e)}")
# #                         raise AirflowException(f"S3 error: {str(e)}")

# #                 @task(task_id=f'download_files')
# #                 def download_files(file_list):
# #                     try:
# #                         landing_dir = os.path.join(CONFIG['landing_path'], format_config['folder'])
# #                         os.makedirs(landing_dir, exist_ok=True)
                        
# #                         downloaded_files = []
# #                         s3 = create_s3_client()
                        
# #                         for file_name in file_list:
# #                             dest_path = os.path.join(landing_dir, file_name)
# #                             s3.download_file(
# #                                 Bucket=CONFIG['s3_bucket'],
# #                                 Key=f"{format_config['folder']}/{file_name}",
# #                                 Filename=dest_path
# #                             )
# #                             downloaded_files.append({
# #                                 'file_name': file_name,
# #                                 'file_path': dest_path,
# #                                 'source_system': 's3',
# #                                 'file_format': file_format
# #                             })
# #                             logger.info(f"Downloaded {file_name} to {dest_path}")
                        
# #                         return downloaded_files
# #                     except Exception as e:
# #                         logger.error(f"Download failed: {str(e)}")
# #                         # Cleanup any partially downloaded files
# #                         for file in downloaded_files:
# #                             if os.path.exists(file['file_path']):
# #                                 os.remove(file['file_path'])
# #                         raise AirflowException(f"Download error: {str(e)}")

# #                 @task(task_id=f'register_files')
# #                 def register_files(files):
# #                     try:
# #                         hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
# #                         registered_files = []
                        
# #                         for file in files:
# #                             # Insert or update file record
# #                             file_id = hook.run(
# #                                 """
# #                                 INSERT INTO dv_stg_files 
# #                                 (source_system, file_format, file_name, file_path)
# #                                 VALUES (%s, %s, %s, %s)
# #                                 ON DUPLICATE KEY UPDATE 
# #                                     file_path = VALUES(file_path),
# #                                     status = 'pending',
# #                                     error_message = NULL
# #                                 """,
# #                                 autocommit=True,
# #                                 parameters=(
# #                                     file['source_system'],
# #                                     file['file_format'],
# #                                     file['file_name'],
# #                                     file['file_path']
# #                                 )
# #                             )
# #                             # Get the file ID
# #                             file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
# #                             registered_files.append({
# #                                 **file,
# #                                 'file_id': file_id
# #                             })
# #                             logger.info(f"Registered file {file['file_name']} with ID {file_id}")
                        
# #                         return registered_files
# #                     except Exception as e:
# #                         logger.error(f"Registration failed: {str(e)}")
# #                         raise AirflowException(f"Registration error: {str(e)}")

# #                 @task(task_id=f'stage_files')
# #                 def stage_files(files):
# #                     for file in files:
# #                         try:
# #                             # Process file data (local file)
# #                             if file_format == 'csv':
# #                                 df = pd.read_csv(file['file_path'])
# #                             elif file_format == 'json':
# #                                 df = pd.read_json(file['file_path'])
# #                             elif file_format == 'excel':
# #                                 df = pd.read_excel(file['file_path'])
                            
# #                             data = validate_employee_data(df).to_dict('records')
                            
# #                             load_to_staging_table(
# #                                 CONFIG['mysql_conn_id'],
# #                                 data,
# #                                 {
# #                                     'file_id': file['file_id'],
# #                                     'file_name': file['file_name'],
# #                                     'source_system': file['source_system']
# #                                 }
# #                             )
# #                         except Exception as e:
# #                             logger.error(f"Staging failed for {file['file_name']}: {str(e)}")
# #                             raise
# #                     return files

# #                 @task(task_id=f'process_staging')
# #                 def process_staging(files):
# #                     for file in files:
# #                         try:
# #                             process_staging_to_main(
# #                                 CONFIG['mysql_conn_id'],
# #                                 {
# #                                     'file_id': file['file_id'],
# #                                     'file_name': file['file_name'],
# #                                     'source_system': file['source_system']
# #                                 }
# #                             )
# #                         except Exception as e:
# #                             logger.error(f"Processing failed for {file['file_name']}: {str(e)}")
# #                             raise
# #                     return files

# #                 @task(task_id=f'archive_files')
# #                 def archive_files(files):
# #                     archived_paths = []
# #                     for file in files:
# #                         try:
# #                             source_dir = os.path.dirname(file['file_path'])
# #                             file_name = file['file_name']
# #                             timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
# #                             archive_dir = os.path.join(
# #                                 CONFIG['archive_path'],
# #                                 format_config['folder'],
# #                                 timestamp
# #                             )
# #                             zip_path = create_archive(source_dir, archive_dir, [file_name])
                            
# #                             # Log archive operation
# #                             hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
# #                             hook.run(
# #                                 """
# #                                 INSERT INTO dv_employee_load_audit 
# #                                 (file_name, source_system, records_processed, status)
# #                                 VALUES (%s, %s, %s, 'archived')
# #                                 """,
# #                                 autocommit=True,
# #                                 parameters=(file_name, 's3', 1)
# #                             )
                            
# #                             archived_paths.append(zip_path)
# #                             logger.info(f"Archived {file_name} to {zip_path}")
# #                         except Exception as e:
# #                             logger.error(f"Archiving failed for {file_name}: {str(e)}")
# #                             raise
# #                     return archived_paths

# #                 @task(task_id=f'cleanup_files')
# #                 def cleanup_files(archived_files):
# #                     try:
# #                         s3 = create_s3_client()
# #                         cleaned_count = 0
                        
# #                         for file in archived_files:
# #                             # Cleanup local file
# #                             if os.path.exists(file['file_path']):
# #                                 os.remove(file['file_path'])
# #                                 logger.info(f"Removed local file {file['file_path']}")
                            
# #                             # Cleanup S3 file
# #                             s3.delete_object(
# #                                 Bucket=CONFIG['s3_bucket'],
# #                                 Key=f"{format_config['folder']}/{file['file_name']}"
# #                             )
# #                             logger.info(f"Deleted S3 file {file['file_name']}")
# #                             cleaned_count += 1
                        
# #                         logger.info(f"Cleaned up {cleaned_count} files")
# #                         return True
# #                     except Exception as e:
# #                         logger.error(f"Cleanup failed: {str(e)}")
# #                         raise AirflowException(f"Cleanup error: {str(e)}")

# #                 @task(task_id=f'notify_completion', outlets=[ARCHIVE_DS])
# #                 def notify_completion():
# #                     logger.info(f"Completed processing {file_format} files from S3")
# #                     return True

# #                 # Task dependencies
# #                 file_list = list_files()
# #                 downloaded = download_files(file_list)
# #                 registered = register_files(downloaded)
# #                 staged = stage_files(registered)
# #                 processed = process_staging(staged)
# #                 archived = archive_files(processed)
# #                 cleaned = cleanup_files(processed)
# #                 notify = notify_completion()
                
# #                 file_list >> downloaded >> registered >> staged >> processed >> archived >> cleaned >> notify
            
# #             process_s3_format()

# #     @task_group(group_id='gdrive_processing')
# #     def process_gdrive():
# #         # Create separate task groups with proper closure
# #         def create_format_processor(file_format, format_config):
# #             @task_group(group_id=f'gdrive_{file_format}')
# #             def process_gdrive_format():
# #                 @task(task_id=f'list_files')
# #                 def list_files():
# #                     try:
# #                         logger.info(f"Processing {file_format} files from folder {format_config['landing_folder_id']}")
# #                         logger.info(f"Expected extensions: {format_config['extensions']}")
                        
# #                         files = list_gdrive_files(
# #                             format_config['landing_folder_id'],
# #                             format_config['extensions']
# #                         )
                        
# #                         if not files:
# #                             logger.warning(f"No {file_format} files found in folder {format_config['landing_folder_id']}")
# #                             return []
                        
# #                         logger.info(f"Found {len(files)} {file_format} files: {[f['name'] for f in files]}")
# #                         return files
# #                     except Exception as e:
# #                         logger.error(f"Failed to list {file_format} files: {str(e)}")
# #                         raise AirflowException(f"File listing error: {str(e)}")

# #                 @task(task_id=f'process_files')
# #                 def process_files(files):
# #                     processed_files = []
# #                     for file in files:
# #                         try:
# #                             # Verify file extension before processing
# #                             file_ext = os.path.splitext(file['name'])[1].lower()
# #                             if not any(file_ext == ext.lower() for ext in format_config['extensions']):
# #                                 logger.error(f"File {file['name']} has invalid extension {file_ext} for format {file_format}")
# #                                 continue
                                
# #                             # Process file data directly from GDrive
# #                             data = process_employee_file(file['id'], file['name'], file_format)
                            
# #                             # Register file in database
# #                             hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
# #                             file_id = hook.run(
# #                                 """
# #                                 INSERT INTO dv_stg_files 
# #                                 (source_system, file_format, file_name, file_path, status)
# #                                 VALUES (%s, %s, %s, %s, 'staged')
# #                                 """,
# #                                 autocommit=True,
# #                                 parameters=(
# #                                     'gdrive',
# #                                     file_format,
# #                                     file['name'],
# #                                     f"gdrive:{file['id']}"
# #                                 )
# #                             )
                            
# #                             # Get the file ID
# #                             file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                            
# #                             # Load to staging table
# #                             load_to_staging_table(
# #                                 CONFIG['mysql_conn_id'],
# #                                 data,
# #                                 {
# #                                     'file_id': file_id,
# #                                     'file_name': file['name'],
# #                                     'source_system': 'gdrive'
# #                                 }
# #                             )
                            
# #                             processed_files.append({
# #                                 'file_id': file['id'],
# #                                 'file_name': file['name'],
# #                                 'db_file_id': file_id
# #                             })
                            
# #                         except Exception as e:
# #                             logger.error(f"Failed to process {file['name']}: {str(e)}")
# #                             raise
                    
# #                     return processed_files

# #                 @task(task_id=f'move_to_main')
# #                 def move_to_main(files):
# #                     for file in files:
# #                         try:
# #                             process_staging_to_main(
# #                                 CONFIG['mysql_conn_id'],
# #                                 {
# #                                     'file_id': file['db_file_id'],
# #                                     'file_name': file['file_name'],
# #                                     'source_system': 'gdrive'
# #                                 }
# #                             )
# #                         except Exception as e:
# #                             logger.error(f"Failed to move {file['file_name']} to main: {str(e)}")
# #                             raise
# #                     return files

# #                 @task(task_id=f'archive_files')
# #                 def archive_files(files):
# #                     for file in files:
# #                         try:
# #                             # Move file to archive folder
# #                             move_gdrive_file(file['file_id'], format_config['archive_folder_id'])
                            
# #                             # Log archive operation
# #                             hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
# #                             hook.run(
# #                                 """
# #                                 INSERT INTO dv_employee_load_audit 
# #                                 (file_name, source_system, records_processed, status)
# #                                 VALUES (%s, %s, %s, 'archived')
# #                                 """,
# #                                 autocommit=True,
# #                                 parameters=(file['file_name'], 'gdrive', 1)
# #                             )
                            
# #                             logger.info(f"Archived {file['file_name']}")
# #                         except Exception as e:
# #                             logger.error(f"Failed to archive {file['file_name']}: {str(e)}")
# #                             raise
# #                     return True

# #                 # Task dependencies
# #                 file_list = list_files()
# #                 processed = process_files(file_list)
# #                 moved = move_to_main(processed)
# #                 archived = archive_files(processed)
                
# #                 file_list >> processed >> moved >> archived
            
# #             return process_gdrive_format
        
# #         for file_format, format_config in CONFIG['file_formats'].items():
# #             create_format_processor(file_format, format_config)()

# #     @task(task_id='clean_staging_tables')
# #     def clean_staging_tables():
# #         """Clean up staging tables after all processing is complete"""
# #         try:
# #             hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
            
# #             # Clean stg_employees
# #             deleted_count = hook.run("DELETE FROM dv_stg_employees")
# #             logger.info(f"Cleaned stg_employees table - removed {deleted_count} records")
            
# #             # Clean stg_files
# #             deleted_count = hook.run("DELETE FROM dv_stg_files WHERE status = 'processed'")
# #             logger.info(f"Cleaned stg_files table - removed {deleted_count} processed records")
            
# #             return True
# #         except Exception as e:
# #             logger.error(f"Failed to clean staging tables: {str(e)}")
# #             raise AirflowException(f"Staging table cleanup failed: {str(e)}")

# #     @task(task_id='send_final_notification')
# #     def send_final_notification():
# #         logger.info("All data processing completed successfully")
# #         return True

# #     @task(task_id='verify_gdrive_connection')
# #     def verify_gdrive_connection():
# #         """Verify Google Drive connection before processing"""
# #         try:
# #             service = get_gdrive_service()
# #             results = service.files().list(
# #                 pageSize=1,
# #                 fields="files(id, name)"
# #             ).execute()
# #             logger.info("Google Drive connection successful")
# #             logger.info(f"Test file: {results.get('files', [])}")
# #             return True
# #         except Exception as e:
# #             logger.error("Google Drive connection test failed!")
# #             logger.error(f"Error details: {str(e)}")
# #             raise AirflowException("Google Drive connection verification failed")

# #     # DAG dependencies
# #     verify_conn = verify_gdrive_connection()
# #     create_tables_task = create_tables
# #     processing = [process_s3(), process_gdrive()]
# #     clean_staging = clean_staging_tables()
# #     final_notification = send_final_notification()

# #     verify_conn >> create_tables_task >> processing
# #     processing >> clean_staging >> final_notification

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task, task_group
from airflow.models.dataset import Dataset
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import boto3
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io
import os
import zipfile
import pandas as pd
import logging
import re
import time
import json
from airflow.exceptions import AirflowException
from io import BytesIO

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define datasets for triggering
LANDING_ZONE_DS = Dataset("file://landing_zone")
ARCHIVE_DS = Dataset("file://archive")

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': True,
}

CONFIG = {
    # Database connection
    'mysql_conn_id': "mysql_conn",
    
    # S3 connection
    's3_conn_id': "aws_s3_conn",
    's3_bucket': "fug-de-training",
    's3_landing_prefix': "dv_prac/landing folder",
    's3_archive_prefix': "dv_prac/archive folder",
    
    # Google Drive folder structure
    'gdrive_landing_root': '1Hn74Ia7cU8LJmMAiJlYreptEPjoP-MsG',
    'gdrive_archive_root': '1KkXT7jhpbO-MaucHw6lhWtWdlBZsf9TK',
    
    # File formats configuration
    'file_formats': {
        'csv': {
            'folder': 'csv_data',
            'extensions': ['.csv'],
            'landing_folder_id': '13ZV6C8tT1P2sG8uu2bg19w3uI-aCSEIb',
            'archive_folder_id': '1z00QnPZJMeR1Kuz1Ba9f6gobT8YLOUni'
        },
        'json': {
            'folder': 'json_data',
            'extensions': ['.json'],
            'landing_folder_id': '1yd3gaLu46SKrdZMWERWeNIsUua0yJQJ3',
            'archive_folder_id': '1oJ_H5pTvHcMMuXREE_TxbRGjgu7CTXGD'
        },
        'excel': {
            'folder': 'excel_data',
            'extensions': ['.xlsx', '.xls'],
            'landing_folder_id': '1HP_Gtfk0S2iA_nh5ZPa7NLh6-Bt8HdZ2',
            'archive_folder_id': '1-mAg1EukJnEuL3QmZ4LSSuIha5PfWzDZ'
        }
    },
    
    # Processing config
    'chunk_size': 10000
}

# Debug: Print folder mappings at startup
logger.info("=== Starting DAG with these folder mappings ===")
for fmt, config in CONFIG['file_formats'].items():
    logger.info(f"{fmt.upper()} Configuration:")
    logger.info(f"  Landing Folder ID: {config['landing_folder_id']}")
    logger.info(f"  Archive Folder ID: {config['archive_folder_id']}")
    logger.info(f"  Valid Extensions: {config['extensions']}")
logger.info("==============================================")

def get_gdrive_service():
    """Create authenticated Google Drive service using Airflow Variable"""
    try:
        creds = Variable.get("gcp_config", deserialize_json=True)
        if not creds:
            raise AirflowException("GCP configuration not found in Airflow Variables")
        
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key',
                         'client_email', 'client_id', 'auth_uri', 'token_uri',
                         'auth_provider_x509_cert_url', 'client_x509_cert_url']
        
        missing_fields = [f for f in required_fields if f not in creds]
        if missing_fields:
            raise AirflowException(f"Missing required fields in GCP config: {missing_fields}")
            
        if 'private_key' in creds and isinstance(creds['private_key'], str):
            creds['private_key'] = creds['private_key'].replace('\\n', '\n')
        
        credentials = service_account.Credentials.from_service_account_info(
            creds,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Google Drive authentication failed: {str(e)}")
        raise AirflowException(f"Google Drive service creation failed: {str(e)}")

def list_gdrive_files(folder_id, expected_extensions):
    """List files in Google Drive folder with strict extension checking"""
    try:
        service = get_gdrive_service()
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            pageSize=100,
            fields="files(id, name, mimeType, fileExtension)"
        ).execute()
        
        files = results.get('files', [])
        filtered_files = []
        
        for file in files:
            # Get extension from name or fileExtension property
            file_ext = os.path.splitext(file['name'])[1].lower()
            if not file_ext and 'fileExtension' in file:
                file_ext = f".{file['fileExtension'].lower()}"
            
            # Check if extension matches expected
            if any(file_ext == ext.lower() for ext in expected_extensions):
                filtered_files.append(file)
            else:
                logger.warning(f"Skipping file {file['name']} with extension {file_ext} - expected {expected_extensions}")
        
        return filtered_files
    except Exception as e:
        logger.error(f"Failed to list files in folder {folder_id}: {str(e)}")
        raise AirflowException(f"Google Drive listing error: {str(e)}")

def create_gdrive_zip_archive(service, file_ids, file_names, target_folder_id):
    """Create a zip archive in Google Drive from multiple files"""
    try:
        # Create an in-memory zip file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for file_id, file_name in zip(file_ids, file_names):
                # Download file content
                request = service.files().get_media(fileId=file_id)
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                file_content.seek(0)
                
                # Add to zip
                zip_file.writestr(file_name, file_content.read())
        
        # Create timestamp for archive name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_name = f"archive_{timestamp}.zip"
        
        # Upload the zip to Google Drive
        zip_buffer.seek(0)
        file_metadata = {
            'name': zip_name,
            'parents': [target_folder_id],
            'mimeType': 'application/zip'
        }
        media = MediaIoBaseUpload(zip_buffer, mimetype='application/zip')
        zip_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        logger.info(f"Created Google Drive zip archive with ID: {zip_file['id']}")
        return True
    except Exception as e:
        logger.error(f"Failed to create Google Drive zip archive: {str(e)}")
        raise AirflowException(f"Google Drive zip archive creation failed: {str(e)}")

def clean_gdrive_folder(service, folder_id):
    """Move all files from a Google Drive folder to trash"""
    try:
        # List all files in the folder
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute()
        files = results.get('files', [])
        
        # Move each file to trash
        for file in files:
            service.files().update(
                fileId=file['id'],
                body={'trashed': True}
            ).execute()
            logger.info(f"Moved to trash: {file['name']}")
        
        logger.info(f"Cleaned {len(files)} files from Google Drive folder {folder_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to clean Google Drive folder: {str(e)}")
        raise AirflowException(f"Google Drive folder cleanup failed: {str(e)}")

def move_gdrive_file(file_id, target_folder_id):
    """Move file to target folder in Google Drive"""
    try:
        service = get_gdrive_service()
        
        # Get current parent folders
        file = service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()
        
        previous_parents = ",".join(file.get('parents', []))
        
        # Move the file
        updated_file = service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        
        return updated_file
    except Exception as e:
        logger.error(f"Failed to move file {file_id}: {str(e)}")
        raise AirflowException(f"Google Drive file move failed: {str(e)}")

def read_gdrive_file(file_id, file_name, file_format):
    """Read file content directly from Google Drive"""
    try:
        service = get_gdrive_service()
        request = service.files().get_media(fileId=file_id)
        
        # Use a buffer to hold the file content
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info(f"Downloaded {int(status.progress() * 100)}% of {file_name}")
        
        file_buffer.seek(0)  # Rewind the buffer
        
        # Read based on file format
        if file_format == 'csv':
            df = pd.read_csv(file_buffer)
        elif file_format == 'json':
            df = pd.read_json(file_buffer)
        elif file_format == 'excel':
            df = pd.read_excel(file_buffer)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
            
        return df
    except Exception as e:
        logger.error(f"Failed to read file {file_name}: {str(e)}")
        raise AirflowException(f"File reading error: {str(e)}")

def validate_employee_data(df):
    """Validate employee data structure"""
    try:
        # Validate employee_id (4-digit)
        df['employee_id'] = df['employee_id'].astype(str)
        invalid_ids = df[~df['employee_id'].str.match(r'^\d{4}$')]
        if not invalid_ids.empty:
            raise ValueError(f"Invalid employee IDs: {invalid_ids['employee_id'].tolist()}")
            
        # Validate name fields
        df['firstname'] = df['firstname'].str.strip()
        df['lastname'] = df['lastname'].str.strip()
        
        name_length_issues = df[
            (df['firstname'].str.len() > 20) | 
            (df['lastname'].str.len() > 20)
        ]
        if not name_length_issues.empty:
            raise ValueError(f"Name length exceeds 20 chars for records: {name_length_issues.index.tolist()}")
        
        # Validate email format
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid_emails = df[~df['email'].str.match(email_regex, na=False)]
        if not invalid_emails.empty:
            raise ValueError(f"Invalid email formats: {invalid_emails['email'].tolist()}")
        
        # Validate salary
        try:
            df['salary'] = pd.to_numeric(df['salary'])
        except ValueError as e:
            raise ValueError(f"Invalid salary values: {str(e)}")
        
        return df
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise

def process_employee_file(file_id, file_name, file_format):
    """Process employee file with validation"""
    try:
        # Read and validate file
        df = read_gdrive_file(file_id, file_name, file_format)
            
        # Validate required columns
        required_cols = {'employee_id', 'firstname', 'lastname', 'salary', 'email'}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise ValueError(f"Missing columns: {missing}")
            
        return validate_employee_data(df).to_dict('records')
    except Exception as e:
        logger.error(f"Failed to process {file_name}: {str(e)}")
        raise AirflowException(f"File processing error: {str(e)}")

def load_to_staging_table(conn_id, data, file_info):
    """Load data to staging table"""
    hook = MySqlHook(mysql_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Prepare batch insert
        values = []
        for record in data:
            values.append((
                record['employee_id'],
                record['firstname'],
                record['lastname'],
                float(record['salary']),
                record['email'],
                file_info['source_system'],
                file_info['file_name']
            ))
        
        # Batch insert
        cursor.executemany("""
            INSERT INTO dv_stg_employees 
            (employee_id, firstname, lastname, salary, email, source_system, file_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, values)
        
        # Update file status
        cursor.execute("""
            UPDATE dv_stg_files 
            SET status = 'staged', processed_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (file_info['file_id'],))
        
        conn.commit()
        logger.info(f"Staged {len(data)} records from {file_info['file_name']}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Staging failed: {str(e)}")
        
        # Update error status
        hook.run("""
            UPDATE dv_stg_files 
            SET status = 'failed', error_message = %s 
            WHERE id = %s
        """, parameters=(str(e)[:500], file_info['file_id']))
        
        raise AirflowException(f"Staging error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def process_staging_to_main(conn_id, file_info):
    """Process data from staging to main table"""
    hook = MySqlHook(mysql_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Get count of records to process
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dv_stg_employees 
            WHERE source_system = %s AND file_name = %s
        """, (file_info['source_system'], file_info['file_name']))
        record_count = cursor.fetchone()[0]
        
        if record_count == 0:
            logger.warning(f"No records found for {file_info['file_name']}")
            return
        
        # Transfer to main table
        cursor.execute("""
            INSERT INTO dv_employees (employee_id, firstname, lastname, salary, email)
            SELECT employee_id, firstname, lastname, salary, email
            FROM dv_stg_employees
            WHERE source_system = %s AND file_name = %s
            ON DUPLICATE KEY UPDATE
                firstname = VALUES(firstname),
                lastname = VALUES(lastname),
                salary = VALUES(salary),
                email = VALUES(email)
        """, (file_info['source_system'], file_info['file_name']))
        
        # Log success
        cursor.execute("""
            INSERT INTO dv_employee_load_audit 
            (file_name, source_system, records_processed, status)
            VALUES (%s, %s, %s, 'processed')
        """, (file_info['file_name'], file_info['source_system'], record_count))
        
        # Cleanup staging
        cursor.execute("""
            DELETE FROM dv_stg_employees
            WHERE source_system = %s AND file_name = %s
        """, (file_info['source_system'], file_info['file_name']))
        
        # Update file status
        cursor.execute("""
            UPDATE dv_stg_files 
            SET status = 'processed', processed_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (file_info['file_id'],))
        
        conn.commit()
        logger.info(f"Processed {record_count} records from {file_info['file_name']}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Processing failed: {str(e)}")
        
        # Update error status
        hook.run("""
            UPDATE dv_stg_files 
            SET status = 'failed', error_message = %s 
            WHERE id = %s
        """, parameters=(str(e)[:500], file_info['file_id']))
        
        raise AirflowException(f"Processing error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def create_s3_client():
    """Create authenticated S3 client"""
    try:
        conn = BaseHook.get_connection(CONFIG['s3_conn_id'])
        return boto3.client(
            's3',
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name=conn.extra_dejson.get('region_name', 'us-west-2')
        )
    except Exception as e:
        logger.error(f"S3 client creation failed: {str(e)}")
        raise AirflowException(f"S3 setup failed: {str(e)}")

def read_s3_file(bucket, key, file_format):
    """Read file content directly from S3"""
    try:
        s3 = create_s3_client()
        obj = s3.get_object(Bucket=bucket, Key=key)
        
        # Read based on file format
        if file_format == 'csv':
            df = pd.read_csv(obj['Body'])
        elif file_format == 'json':
            df = pd.read_json(obj['Body'])
        elif file_format == 'excel':
            df = pd.read_excel(io.BytesIO(obj['Body'].read()))
        else:
            raise ValueError(f"Unsupported format: {file_format}")
            
        return df
    except Exception as e:
        logger.error(f"Failed to read file {key}: {str(e)}")
        raise AirflowException(f"S3 file reading error: {str(e)}")

def create_s3_zip_archive(s3_client, source_bucket, source_keys, target_bucket, target_key):
    """Create a zip archive in S3 from multiple source files"""
    try:
        # Create an in-memory zip file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for source_key in source_keys:
                # Get the file name without path
                file_name = source_key.split('/')[-1]
                
                # Get the file content from S3
                obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
                file_content = obj['Body'].read()
                
                # Add to zip
                zip_file.writestr(file_name, file_content)
        
        # Upload the zip to S3
        zip_buffer.seek(0)
        s3_client.put_object(Bucket=target_bucket, Key=target_key, Body=zip_buffer.getvalue())
        
        logger.info(f"Created archive at s3://{target_bucket}/{target_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to create S3 zip archive: {str(e)}")
        raise AirflowException(f"S3 zip archive creation failed: {str(e)}")

def clean_s3_folder(s3_client, bucket, prefix):
    """Clean all files in an S3 folder"""
    try:
        # List all objects in the folder
        objects_to_delete = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
        
        # Delete all objects
        if objects_to_delete:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            logger.info(f"Cleaned {len(objects_to_delete)} files from s3://{bucket}/{prefix}")
        else:
            logger.info(f"No files found to clean in s3://{bucket}/{prefix}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to clean S3 folder: {str(e)}")
        raise AirflowException(f"S3 folder cleanup failed: {str(e)}")

with DAG(
    'employee_data_processing',
    default_args=default_args,
    schedule=[LANDING_ZONE_DS],
    catchup=False,
    max_active_runs=1,
    tags=['employee_data', 'mysql', 's3', 'gdrive'],
    description='Process employee data from S3 and Google Drive to MySQL',
) as dag:

    create_tables = MySqlOperator(
        task_id='create_tables',
        mysql_conn_id=CONFIG['mysql_conn_id'],
        sql="""
        CREATE TABLE IF NOT EXISTS dv_employees (
            employee_id INT(4) ZEROFILL PRIMARY KEY,
            firstname VARCHAR(20) NOT NULL,
            lastname VARCHAR(20) NOT NULL,
            salary DECIMAL(10,2) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS dv_stg_employees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            employee_id VARCHAR(4) NOT NULL,
            firstname VARCHAR(20) NOT NULL,
            lastname VARCHAR(20) NOT NULL,
            salary DECIMAL(10,2) NOT NULL,
            email VARCHAR(100) NOT NULL,
            source_system VARCHAR(50) NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            staged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_source_file (source_system, file_name)
        );
        
        CREATE TABLE IF NOT EXISTS dv_stg_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source_system VARCHAR(50),
            file_format VARCHAR(10),
            file_name VARCHAR(255),
            file_path VARCHAR(512),
            status VARCHAR(20) DEFAULT 'pending',
            error_message TEXT,
            processed_at TIMESTAMP NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS dv_employee_load_audit (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            source_system VARCHAR(50) NOT NULL,
            records_processed INT NOT NULL,
            status VARCHAR(20) NOT NULL,
            error_message TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    @task_group(group_id='s3_processing')
    def process_s3():
        for file_format, format_config in CONFIG['file_formats'].items():
            @task_group(group_id=f's3_{file_format}')
            def process_s3_format():
                @task(task_id=f'list_files')
                def list_files():
                    try:
                        s3 = create_s3_client()
                        response = s3.list_objects_v2(
                            Bucket=CONFIG['s3_bucket'],
                            Prefix=f"{CONFIG['s3_landing_prefix']}/{format_config['folder']}/"
                        )
                        files = [
                            {'Key': obj['Key'], 'name': obj['Key'].split('/')[-1]}
                            for obj in response.get('Contents', []) 
                            if any(obj['Key'].lower().endswith(ext) for ext in format_config['extensions'])
                        ]
                        logger.info(f"Found {len(files)} {file_format} files in S3")
                        return files
                    except Exception as e:
                        logger.error(f"S3 listing failed: {str(e)}")
                        raise AirflowException(f"S3 error: {str(e)}")

                @task(task_id=f'register_files')
                def register_files(files):
                    try:
                        hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
                        registered_files = []
                        
                        for file in files:
                            # Insert or update file record
                            file_id = hook.run(
                                """
                                INSERT INTO dv_stg_files 
                                (source_system, file_format, file_name, file_path)
                                VALUES (%s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    file_path = VALUES(file_path),
                                    status = 'pending',
                                    error_message = NULL
                                """,
                                autocommit=True,
                                parameters=(
                                    's3',
                                    file_format,
                                    file['name'],
                                    f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
                                )
                            )
                            # Get the file ID
                            file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                            registered_files.append({
                                'file_key': file['Key'],
                                'file_name': file['name'],
                                'source_system': 's3',
                                'file_format': file_format,
                                'file_id': file_id
                            })
                            logger.info(f"Registered file {file['name']} with ID {file_id}")
                        
                        return registered_files
                    except Exception as e:
                        logger.error(f"Registration failed: {str(e)}")
                        raise AirflowException(f"Registration error: {str(e)}")

                @task(task_id=f'process_files')
                def process_files(registered_files):
                    processed_files = []
                    for file in registered_files:
                        try:
                            # Read file directly from S3
                            df = read_s3_file(
                                CONFIG['s3_bucket'],
                                file['file_key'],
                                file_format
                            )
                            
                            # Validate data (same as Google Drive validation)
                            data = validate_employee_data(df).to_dict('records')
                            
                            # Load to staging table
                            load_to_staging_table(
                                CONFIG['mysql_conn_id'],
                                data,
                                {
                                    'file_id': file['file_id'],
                                    'file_name': file['file_name'],
                                    'source_system': file['source_system']
                                }
                            )
                            
                            processed_files.append(file)
                        except Exception as e:
                            logger.error(f"Processing failed for {file['file_name']}: {str(e)}")
                            raise
                    return processed_files

                @task(task_id=f'move_to_main')
                def move_to_main(processed_files):
                    for file in processed_files:
                        try:
                            process_staging_to_main(
                                CONFIG['mysql_conn_id'],
                                {
                                    'file_id': file['file_id'],
                                    'file_name': file['file_name'],
                                    'source_system': file['source_system']
                                }
                            )
                        except Exception as e:
                            logger.error(f"Failed to move {file['file_name']} to main: {str(e)}")
                            raise
                    return processed_files

                @task(task_id=f'archive_files')
                def archive_files(processed_files):
                    try:
                        if not processed_files:
                            logger.info("No files to archive")
                            return True
                            
                        s3 = create_s3_client()
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        
                        # Prepare archive details
                        source_keys = [file['file_key'] for file in processed_files]
                        archive_folder = f"{CONFIG['s3_archive_prefix']}/{format_config['folder']}"
                        archive_key = f"{archive_folder}/archive_{timestamp}.zip"
                        
                        # Create zip archive in S3
                        create_s3_zip_archive(
                            s3,
                            CONFIG['s3_bucket'],
                            source_keys,
                            CONFIG['s3_bucket'],
                            archive_key
                        )
                        
                        # Log archive operation
                        hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
                        for file in processed_files:
                            hook.run(
                                """
                                INSERT INTO dv_employee_load_audit 
                                (file_name, source_system, records_processed, status)
                                VALUES (%s, %s, %s, 'archived')
                                """,
                                autocommit=True,
                                parameters=(file['file_name'], 's3', 1)
                            )
                        
                        logger.info(f"Archived {len(processed_files)} files to {archive_key}")
                        return True
                    except Exception as e:
                        logger.error(f"Archiving failed: {str(e)}")
                        raise

                @task(task_id=f'clean_landing_folder')
                def clean_landing_folder(processed_files):
                    try:
                        if not processed_files:
                            logger.info("No files to clean")
                            return True
                            
                        s3 = create_s3_client()
                        landing_prefix = f"{CONFIG['s3_landing_prefix']}/{format_config['folder']}/"
                        
                        # Clean the entire landing folder for this format
                        clean_s3_folder(
                            s3,
                            CONFIG['s3_bucket'],
                            landing_prefix
                        )
                        
                        logger.info(f"Cleaned landing folder: {landing_prefix}")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to clean landing folder: {str(e)}")
                        raise

                @task(task_id=f'notify_completion', outlets=[ARCHIVE_DS])
                def notify_completion():
                    logger.info(f"Completed processing {file_format} files from S3")
                    return True

                # Task dependencies
                file_list = list_files()
                registered = register_files(file_list)
                processed = process_files(registered)
                moved = move_to_main(processed)
                archived = archive_files(processed)
                cleaned = clean_landing_folder(processed)
                notify = notify_completion()
                
                file_list >> registered >> processed >> moved >> archived >> cleaned >> notify
            
            process_s3_format()

    @task_group(group_id='gdrive_processing')
    def process_gdrive():
        # Create separate task groups with proper closure
        def create_format_processor(file_format, format_config):
            @task_group(group_id=f'gdrive_{file_format}')
            def process_gdrive_format():
                @task(task_id=f'list_files')
                def list_files():
                    try:
                        logger.info(f"Processing {file_format} files from folder {format_config['landing_folder_id']}")
                        logger.info(f"Expected extensions: {format_config['extensions']}")
                        
                        files = list_gdrive_files(
                            format_config['landing_folder_id'],
                            format_config['extensions']
                        )
                        
                        if not files:
                            logger.warning(f"No {file_format} files found in folder {format_config['landing_folder_id']}")
                            return []
                        
                        logger.info(f"Found {len(files)} {file_format} files: {[f['name'] for f in files]}")
                        return files
                    except Exception as e:
                        logger.error(f"Failed to list {file_format} files: {str(e)}")
                        raise AirflowException(f"File listing error: {str(e)}")

                @task(task_id=f'process_files')
                def process_files(files):
                    processed_files = []
                    for file in files:
                        try:
                            # Verify file extension before processing
                            file_ext = os.path.splitext(file['name'])[1].lower()
                            if not any(file_ext == ext.lower() for ext in format_config['extensions']):
                                logger.error(f"File {file['name']} has invalid extension {file_ext} for format {file_format}")
                                continue
                                
                            # Process file data directly from GDrive
                            data = process_employee_file(file['id'], file['name'], file_format)
                            
                            # Register file in database
                            hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
                            file_id = hook.run(
                                """
                                INSERT INTO dv_stg_files 
                                (source_system, file_format, file_name, file_path, status)
                                VALUES (%s, %s, %s, %s, 'staged')
                                """,
                                autocommit=True,
                                parameters=(
                                    'gdrive',
                                    file_format,
                                    file['name'],
                                    f"gdrive:{file['id']}"
                                )
                            )
                            
                            # Get the file ID
                            file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                            
                            # Load to staging table
                            load_to_staging_table(
                                CONFIG['mysql_conn_id'],
                                data,
                                {
                                    'file_id': file_id,
                                    'file_name': file['name'],
                                    'source_system': 'gdrive'
                                }
                            )
                            
                            processed_files.append({
                                'file_id': file['id'],
                                'file_name': file['name'],
                                'db_file_id': file_id
                            })
                            
                        except Exception as e:
                            logger.error(f"Failed to process {file['name']}: {str(e)}")
                            raise
                    
                    return processed_files

                @task(task_id=f'move_to_main')
                def move_to_main(files):
                    for file in files:
                        try:
                            process_staging_to_main(
                                CONFIG['mysql_conn_id'],
                                {
                                    'file_id': file['db_file_id'],
                                    'file_name': file['file_name'],
                                    'source_system': 'gdrive'
                                }
                            )
                        except Exception as e:
                            logger.error(f"Failed to move {file['file_name']} to main: {str(e)}")
                            raise
                    return files

                @task(task_id=f'archive_files')
                def archive_files(files):
                    try:
                        if not files:
                            logger.info("No files to archive")
                            return True
                            
                        service = get_gdrive_service()
                        
                        # Create zip archive in Google Drive
                        create_gdrive_zip_archive(
                            service,
                            [file['file_id'] for file in files],
                            [file['file_name'] for file in files],
                            format_config['archive_folder_id']
                        )
                        
                        # Log archive operation
                        hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
                        for file in files:
                            hook.run(
                                """
                                INSERT INTO dv_employee_load_audit 
                                (file_name, source_system, records_processed, status)
                                VALUES (%s, %s, %s, 'archived')
                                """,
                                autocommit=True,
                                parameters=(file['file_name'], 'gdrive', 1)
                            )
                        
                        logger.info(f"Archived {len(files)} files to Google Drive")
                        return True
                    except Exception as e:
                        logger.error(f"Archiving failed: {str(e)}")
                        raise

                @task(task_id=f'clean_landing_folder')
                def clean_landing_folder(files):
                    try:
                        if not files:
                            logger.info("No files to clean")
                            return True
                            
                        service = get_gdrive_service()
                        clean_gdrive_folder(service, format_config['landing_folder_id'])
                        logger.info(f"Cleaned Google Drive landing folder")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to clean landing folder: {str(e)}")
                        raise

                @task(task_id=f'notify_completion', outlets=[ARCHIVE_DS])
                def notify_completion():
                    logger.info(f"Completed processing {file_format} files from Google Drive")
                    return True

                # Task dependencies
                file_list = list_files()
                processed = process_files(file_list)
                moved = move_to_main(processed)
                archived = archive_files(processed)
                cleaned = clean_landing_folder(processed)
                notify = notify_completion()
                
                file_list >> processed >> moved >> archived >> cleaned >> notify
            
            return process_gdrive_format
        
        for file_format, format_config in CONFIG['file_formats'].items():
            create_format_processor(file_format, format_config)()

    @task(task_id='clean_staging_tables')
    def clean_staging_tables():
        """Clean up staging tables after all processing is complete"""
        try:
            hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
            
            # Clean stg_employees
            deleted_count = hook.run("DELETE FROM dv_stg_employees")
            logger.info(f"Cleaned stg_employees table - removed {deleted_count} records")
            
            # Clean stg_files
            deleted_count = hook.run("DELETE FROM dv_stg_files WHERE status = 'processed'")
            logger.info(f"Cleaned stg_files table - removed {deleted_count} processed records")
            
            return True
        except Exception as e:
            logger.error(f"Failed to clean staging tables: {str(e)}")
            raise AirflowException(f"Staging table cleanup failed: {str(e)}")

    @task(task_id='send_final_notification')
    def send_final_notification():
        logger.info("All data processing completed successfully")
        return True

    @task(task_id='verify_gdrive_connection')
    def verify_gdrive_connection():
        """Verify Google Drive connection before processing"""
        try:
            service = get_gdrive_service()
            results = service.files().list(
                pageSize=1,
                fields="files(id, name)"
            ).execute()
            logger.info("Google Drive connection successful")
            logger.info(f"Test file: {results.get('files', [])}")
            return True
        except Exception as e:
            logger.error("Google Drive connection test failed!")
            logger.error(f"Error details: {str(e)}")
            raise AirflowException("Google Drive connection verification failed")

    # DAG dependencies
    verify_conn = verify_gdrive_connection()
    create_tables_task = create_tables
    processing = [process_s3(), process_gdrive()]
    clean_staging = clean_staging_tables()
    final_notification = send_final_notification()

    verify_conn >> create_tables_task >> processing
    processing >> clean_staging >> final_notification

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.models import Variable
# from airflow.decorators import task, task_group
# from airflow.models.dataset import Dataset
# from airflow.providers.mysql.operators.mysql import MySqlOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.hooks.base import BaseHook
# import boto3
# from botocore.exceptions import ClientError
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
# import io
# import os
# import zipfile
# import pandas as pd
# import logging
# import re
# import time
# import json
# from airflow.exceptions import AirflowException
# from io import BytesIO


# # Configure logging
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

# # Define datasets for triggering
# LANDING_ZONE_DS = Dataset("file://landing_zone")
# ARCHIVE_DS = Dataset("file://archive")

# default_args = {
#     'owner': 'data_team',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
#     'email_on_failure': True,
#     'email_on_retry': True,
# }

# # Hardcoded configuration with explicit paths
# CONFIG = {
#     'mysql_conn_id': "mysql_conn",
#     's3_conn_id': "aws_s3_conn",
#     's3_bucket': "fug-de-training",
#     's3_paths': {
#         'csv': {
#             'landing': "dv_prac/landing_folder/csv_data/",
#             'archive': "dv_prac/archive_folder/csv_data/",
#             'extensions': ['.csv']
#         },
#         'json': {
#             'landing': "dv_prac/landing_folder/json_data/",
#             'archive': "dv_prac/archive_folder/json_data/",
#             'extensions': ['.json']
#         },
#         'excel': {
#             'landing': "dv_prac/landing_folder/excel_data/",
#             'archive': "dv_prac/archive_folder/excel_data/",
#             'extensions': ['.xlsx', '.xls']
#         }
#     },
#     'gdrive_paths': {
#         'csv': {
#             'landing_folder_id': '13ZV6C8tT1P2sG8uu2bg19w3uI-aCSEIb',
#             'archive_folder_id': '1z00QnPZJMeR1Kuz1Ba9f6gobT8YLOUni',
#             'extensions': ['.csv']
#         },
#         'json': {
#             'landing_folder_id': '1yd3gaLu46SKrdZMWERWeNIsUua0yJQJ3',
#             'archive_folder_id': '1oJ_H5pTvHcMMuXREE_TxbRGjgu7CTXGD',
#             'extensions': ['.json']
#         },
#         'excel': {
#             'landing_folder_id': '1HP_Gtfk0S2iA_nh5ZPa7NLh6-Bt8HdZ2',
#             'archive_folder_id': '1-mAg1EukJnEuL3QmZ4LSSuIha5PfWzDZ',
#             'extensions': ['.xlsx', '.xls']
#         }
#     },
#     'chunk_size': 10000
# }

# def get_gdrive_service():
#     """Create authenticated Google Drive service"""
#     try:
#         creds = Variable.get("gcp_config", deserialize_json=True)
#         if not creds:
#             raise AirflowException("GCP configuration not found in Airflow Variables")
        
#         credentials = service_account.Credentials.from_service_account_info(
#             creds,
#             scopes=['https://www.googleapis.com/auth/drive']
#         )
        
#         return build('drive', 'v3', credentials=credentials)
#     except Exception as e:
#         logger.error(f"Google Drive authentication failed: {str(e)}")
#         raise AirflowException(f"Google Drive service creation failed: {str(e)}")

# def list_gdrive_files(folder_id, expected_extensions, format_type):
#     """List files in Google Drive folder"""
#     try:
#         service = get_gdrive_service()
#         results = service.files().list(
#             q=f"'{folder_id}' in parents and trashed=false",
#             pageSize=100,
#             fields="files(id, name, mimeType, fileExtension)"
#         ).execute()
        
#         files = results.get('files', [])
#         filtered_files = []
        
#         for file in files:
#             file_ext = os.path.splitext(file['name'])[1].lower()
#             if not file_ext and 'fileExtension' in file:
#                 file_ext = f".{file['fileExtension'].lower()}"
            
#             if any(file_ext == ext.lower() for ext in expected_extensions):
#                 filtered_files.append(file)
        
#         return filtered_files
#     except Exception as e:
#         logger.error(f"Failed to list {format_type} files: {str(e)}")
#         raise AirflowException(f"Google Drive {format_type} listing error: {str(e)}")

# def create_gdrive_zip_archive(service, file_ids, file_names, target_folder_id, format_type):
#     """Create a zip archive in Google Drive from multiple files"""
#     try:
#         # Create an in-memory zip file
#         zip_buffer = BytesIO()
#         with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
#             for file_id, file_name in zip(file_ids, file_names):
#                 # Download file content
#                 request = service.files().get_media(fileId=file_id)
#                 file_content = io.BytesIO()
#                 downloader = MediaIoBaseDownload(file_content, request)
#                 done = False
#                 while not done:
#                     _, done = downloader.next_chunk()
#                 file_content.seek(0)
                
#                 # Add to zip
#                 zip_file.writestr(file_name, file_content.read())
        
#         # Create timestamp for archive name
#         timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#         zip_name = f"{format_type}_archive_{timestamp}.zip"
        
#         # Upload the zip to Google Drive
#         zip_buffer.seek(0)
#         file_metadata = {
#             'name': zip_name,
#             'parents': [target_folder_id],
#             'mimeType': 'application/zip'
#         }
#         media = MediaIoBaseUpload(zip_buffer, mimetype='application/zip')
#         zip_file = service.files().create(
#             body=file_metadata,
#             media_body=media,
#             fields='id'
#         ).execute()
        
#         logger.info(f"Created {format_type} Google Drive zip archive with ID: {zip_file['id']}")
#         return zip_file['id']
#     except HttpError as e:
#         if 'storageQuotaExceeded' in str(e):
#             raise
#         logger.error(f"Failed to create {format_type} Google Drive zip archive: {str(e)}")
#         raise AirflowException(f"Google Drive {format_type} zip archive creation failed: {str(e)}")
#     except Exception as e:
#         logger.error(f"Failed to create {format_type} Google Drive zip archive: {str(e)}")
#         raise AirflowException(f"Google Drive {format_type} zip archive creation failed: {str(e)}")
    
# # def create_gdrive_zip_archive(service, file_ids, file_names, target_folder_id, format_type):
# #     """Create zip archive in Google Drive"""
# #     try:
# #         zip_buffer = BytesIO()
# #         with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
# #             for file_id, file_name in zip(file_ids, file_names):
# #                 request = service.files().get_media(fileId=file_id)
# #                 file_content = io.BytesIO()
# #                 downloader = MediaIoBaseDownload(file_content, request)
# #                 done = False
# #                 while not done:
# #                     _, done = downloader.next_chunk()
# #                 file_content.seek(0)
# #                 zip_file.writestr(file_name, file_content.read())
        
# #         timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
# #         zip_name = f"{format_type}_archive_{timestamp}.zip"
        
# #         zip_buffer.seek(0)
# #         file_metadata = {
# #             'name': zip_name,
# #             'parents': [target_folder_id],
# #             'mimeType': 'application/zip'
# #         }
# #         media = MediaIoBaseUpload(zip_buffer, mimetype='application/zip')
# #         zip_file = service.files().create(
# #             body=file_metadata,
# #             media_body=media,
# #             fields='id'
# #         ).execute()
        
# #         return zip_file['id']
# #     except Exception as e:
# #         logger.error(f"Failed to create {format_type} zip archive: {str(e)}")
# #         raise AirflowException(f"Google Drive {format_type} zip creation failed: {str(e)}")

# def move_gdrive_file(file_id, target_folder_id, format_type):
#     """Move file in Google Drive"""
#     try:
#         service = get_gdrive_service()
#         file = service.files().get(fileId=file_id, fields='parents').execute()
#         previous_parents = ",".join(file.get('parents', []))
        
#         updated_file = service.files().update(
#             fileId=file_id,
#             addParents=target_folder_id,
#             removeParents=previous_parents,
#             fields='id, parents'
#         ).execute()
        
#         return updated_file
#     except Exception as e:
#         logger.error(f"Failed to move {format_type} file: {str(e)}")
#         raise AirflowException(f"Google Drive {format_type} file move failed: {str(e)}")

# def read_gdrive_file(file_id, file_name, format_type):
#     """Read file content from Google Drive"""
#     try:
#         service = get_gdrive_service()
#         request = service.files().get_media(fileId=file_id)
#         file_buffer = io.BytesIO()
#         downloader = MediaIoBaseDownload(file_buffer, request)
        
#         done = False
#         while not done:
#             _, done = downloader.next_chunk()
        
#         file_buffer.seek(0)
        
#         if format_type == 'csv':
#             df = pd.read_csv(file_buffer)
#         elif format_type == 'json':
#             df = pd.read_json(file_buffer)
#         elif format_type == 'excel':
#             df = pd.read_excel(file_buffer)
#         else:
#             raise ValueError(f"Unsupported format: {format_type}")
            
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read {format_type} file: {str(e)}")
#         raise AirflowException(f"{format_type} file reading error: {str(e)}")

# def validate_employee_data(df):
#     """Validate employee data structure"""
#     try:
#         df['employee_id'] = df['employee_id'].astype(str)
#         invalid_ids = df[~df['employee_id'].str.match(r'^\d{4}$')]
#         if not invalid_ids.empty:
#             raise ValueError(f"Invalid employee IDs: {invalid_ids['employee_id'].tolist()}")
            
#         df['firstname'] = df['firstname'].str.strip()
#         df['lastname'] = df['lastname'].str.strip()
        
#         name_length_issues = df[
#             (df['firstname'].str.len() > 20) | 
#             (df['lastname'].str.len() > 20)
#         ]
#         if not name_length_issues.empty:
#             raise ValueError(f"Name length exceeds 20 chars for records: {name_length_issues.index.tolist()}")
        
#         email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
#         invalid_emails = df[~df['email'].str.match(email_regex, na=False)]
#         if not invalid_emails.empty:
#             raise ValueError(f"Invalid email formats: {invalid_emails['email'].tolist()}")
        
#         try:
#             df['salary'] = pd.to_numeric(df['salary'])
#         except ValueError as e:
#             raise ValueError(f"Invalid salary values: {str(e)}")
        
#         return df
#     except Exception as e:
#         logger.error(f"Data validation failed: {str(e)}")
#         raise

# def process_employee_file(file_id, file_name, format_type):
#     """Process employee file with validation"""
#     try:
#         df = read_gdrive_file(file_id, file_name, format_type)
            
#         required_cols = {'employee_id', 'firstname', 'lastname', 'salary', 'email'}
#         if not required_cols.issubset(df.columns):
#             missing = required_cols - set(df.columns)
#             raise ValueError(f"Missing columns: {missing}")
            
#         return validate_employee_data(df).to_dict('records')
#     except Exception as e:
#         logger.error(f"Failed to process {format_type} file: {str(e)}")
#         raise AirflowException(f"{format_type} file processing error: {str(e)}")

# def load_to_staging_table(conn_id, data, file_info):
#     """Load data to staging table"""
#     hook = MySqlHook(mysql_conn_id=conn_id)
#     conn = hook.get_conn()
#     cursor = conn.cursor()
    
#     try:
#         values = []
#         for record in data:
#             values.append((
#                 record['employee_id'],
#                 record['firstname'],
#                 record['lastname'],
#                 float(record['salary']),
#                 record['email'],
#                 file_info['source_system'],
#                 file_info['file_name']
#             ))
        
#         cursor.executemany("""
#             INSERT INTO dv_stg_employees 
#             (employee_id, firstname, lastname, salary, email, source_system, file_name)
#             VALUES (%s, %s, %s, %s, %s, %s, %s)
#         """, values)
        
#         cursor.execute("""
#             UPDATE dv_stg_files 
#             SET status = 'staged', processed_at = CURRENT_TIMESTAMP 
#             WHERE id = %s
#         """, (file_info['file_id'],))
        
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         hook.run("""
#             UPDATE dv_stg_files 
#             SET status = 'failed', error_message = %s 
#             WHERE id = %s
#         """, parameters=(str(e)[:500], file_info['file_id']))
#         raise AirflowException(f"Staging error: {str(e)}")
#     finally:
#         cursor.close()
#         conn.close()

# def process_staging_to_main(conn_id, file_info):
#     """Process data from staging to main table"""
#     hook = MySqlHook(mysql_conn_id=conn_id)
#     conn = hook.get_conn()
#     cursor = conn.cursor()
    
#     try:
#         cursor.execute("""
#             SELECT COUNT(*) 
#             FROM dv_stg_employees 
#             WHERE source_system = %s AND file_name = %s
#         """, (file_info['source_system'], file_info['file_name']))
#         record_count = cursor.fetchone()[0]
        
#         if record_count == 0:
#             logger.warning(f"No records found for {file_info['file_name']}")
#             return
        
#         cursor.execute("""
#             INSERT INTO dv_employees (employee_id, firstname, lastname, salary, email)
#             SELECT employee_id, firstname, lastname, salary, email
#             FROM dv_stg_employees
#             WHERE source_system = %s AND file_name = %s
#             ON DUPLICATE KEY UPDATE
#                 firstname = VALUES(firstname),
#                 lastname = VALUES(lastname),
#                 salary = VALUES(salary),
#                 email = VALUES(email)
#         """, (file_info['source_system'], file_info['file_name']))
        
#         cursor.execute("""
#             INSERT INTO dv_employee_load_audit 
#             (file_name, source_system, records_processed, status)
#             VALUES (%s, %s, %s, 'processed')
#         """, (file_info['file_name'], file_info['source_system'], record_count))
        
#         cursor.execute("""
#             DELETE FROM dv_stg_employees
#             WHERE source_system = %s AND file_name = %s
#         """, (file_info['source_system'], file_info['file_name']))
        
#         cursor.execute("""
#             UPDATE dv_stg_files 
#             SET status = 'processed', processed_at = CURRENT_TIMESTAMP 
#             WHERE id = %s
#         """, (file_info['file_id'],))
        
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         hook.run("""
#             UPDATE dv_stg_files 
#             SET status = 'failed', error_message = %s 
#             WHERE id = %s
#         """, parameters=(str(e)[:500], file_info['file_id']))
#         raise AirflowException(f"Processing error: {str(e)}")
#     finally:
#         cursor.close()
#         conn.close()

# def create_s3_client():
#     """Create authenticated S3 client"""
#     try:
#         conn = BaseHook.get_connection(CONFIG['s3_conn_id'])
#         return boto3.client(
#             's3',
#             aws_access_key_id=conn.login,
#             aws_secret_access_key=conn.password,
#             region_name=conn.extra_dejson.get('region_name', 'us-west-2')
#         )
#     except Exception as e:
#         logger.error(f"S3 client creation failed: {str(e)}")
#         raise AirflowException(f"S3 setup failed: {str(e)}")

# def read_s3_file(bucket, key, format_type):
#     """Read file content from S3"""
#     try:
#         s3 = create_s3_client()
#         obj = s3.get_object(Bucket=bucket, Key=key)
        
#         if format_type == 'csv':
#             df = pd.read_csv(obj['Body'])
#         elif format_type == 'json':
#             df = pd.read_json(obj['Body'])
#         elif format_type == 'excel':
#             df = pd.read_excel(io.BytesIO(obj['Body'].read()))
#         else:
#             raise ValueError(f"Unsupported format: {format_type}")
            
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read {format_type} file: {str(e)}")
#         raise AirflowException(f"S3 {format_type} file reading error: {str(e)}")

# def create_s3_zip_archive(s3_client, source_bucket, source_keys, target_bucket, target_key, format_type):
#     """Create zip archive in S3"""
#     try:
#         zip_buffer = BytesIO()
#         with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
#             for source_key in source_keys:
#                 file_name = source_key.split('/')[-1]
#                 obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
#                 file_content = obj['Body'].read()
#                 zip_file.writestr(file_name, file_content)
        
#         zip_buffer.seek(0)
#         s3_client.put_object(Bucket=target_bucket, Key=target_key, Body=zip_buffer.getvalue())
#         return True
#     except Exception as e:
#         logger.error(f"Failed to create {format_type} zip archive: {str(e)}")
#         raise AirflowException(f"S3 {format_type} zip archive creation failed: {str(e)}")

# def clean_s3_folder(s3_client, bucket, prefix, format_type):
#     """Clean S3 folder"""
#     try:
#         objects_to_delete = []
#         paginator = s3_client.get_paginator('list_objects_v2')
#         for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
#             if 'Contents' in page:
#                 objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
        
#         if objects_to_delete:
#             s3_client.delete_objects(
#                 Bucket=bucket,
#                 Delete={'Objects': objects_to_delete}
#             )
#         return True
#     except Exception as e:
#         logger.error(f"Failed to clean {format_type} folder: {str(e)}")
#         raise AirflowException(f"S3 {format_type} folder cleanup failed: {str(e)}")

# def ensure_s3_folder_exists(s3_client, bucket, prefix, format_type):
#     """Ensure S3 folder exists"""
#     try:
#         response = s3_client.list_objects_v2(
#             Bucket=bucket,
#             Prefix=prefix,
#             MaxKeys=1
#         )
        
#         if 'Contents' not in response and 'CommonPrefixes' not in response:
#             s3_client.put_object(Bucket=bucket, Key=f"{prefix}/")
#         return True
#     except Exception as e:
#         logger.error(f"Failed to ensure {format_type} folder exists: {str(e)}")
#         raise AirflowException(f"S3 {format_type} folder creation failed: {str(e)}")

# with DAG(
#     'employee_data_processing',
#     default_args=default_args,
#     schedule=[LANDING_ZONE_DS],
#     catchup=False,
#     max_active_runs=1,
#     tags=['employee_data', 'mysql', 's3', 'gdrive'],
#     description='Process employee data from S3 and Google Drive to MySQL',
# ) as dag:

#     create_tables = MySqlOperator(
#         task_id='create_tables',
#         mysql_conn_id=CONFIG['mysql_conn_id'],
#         sql="""
#         CREATE TABLE IF NOT EXISTS dv_employees (
#             employee_id INT(4) ZEROFILL PRIMARY KEY,
#             firstname VARCHAR(20) NOT NULL,
#             lastname VARCHAR(20) NOT NULL,
#             salary DECIMAL(10,2) NOT NULL,
#             email VARCHAR(100) NOT NULL UNIQUE,
#             processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
        
#         CREATE TABLE IF NOT EXISTS dv_stg_employees (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             employee_id VARCHAR(4) NOT NULL,
#             firstname VARCHAR(20) NOT NULL,
#             lastname VARCHAR(20) NOT NULL,
#             salary DECIMAL(10,2) NOT NULL,
#             email VARCHAR(100) NOT NULL,
#             source_system VARCHAR(50) NOT NULL,
#             file_name VARCHAR(255) NOT NULL,
#             staged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             INDEX idx_source_file (source_system, file_name)
#         );
        
#         CREATE TABLE IF NOT EXISTS dv_stg_files (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             source_system VARCHAR(50),
#             file_format VARCHAR(10),
#             file_name VARCHAR(255),
#             file_path VARCHAR(512),
#             status VARCHAR(20) DEFAULT 'pending',
#             error_message TEXT,
#             processed_at TIMESTAMP NULL,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
        
#         CREATE TABLE IF NOT EXISTS dv_employee_load_audit (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             file_name VARCHAR(255) NOT NULL,
#             source_system VARCHAR(50) NOT NULL,
#             records_processed INT NOT NULL,
#             status VARCHAR(20) NOT NULL,
#             error_message TEXT,
#             processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#         """
#     )

#     @task_group(group_id='s3_processing')
#     def process_s3():
#         @task_group(group_id='s3_csv')
#         def process_s3_csv():
#             @task(task_id='list_files')
#             def list_files():
#                 s3 = create_s3_client()
#                 response = s3.list_objects_v2(
#                     Bucket=CONFIG['s3_bucket'],
#                     Prefix=CONFIG['s3_paths']['csv']['landing']
#                 )
#                 return [obj for obj in response.get('Contents', []) 
#                        if not obj['Key'].endswith('/') 
#                        and os.path.splitext(obj['Key'])[1].lower() == '.csv']

#             @task(task_id='register_files')
#             def register_files(files):
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 registered_files = []
#                 for file in files:
#                     file_id = hook.run(
#                         """
#                         INSERT INTO dv_stg_files 
#                         (source_system, file_format, file_name, file_path)
#                         VALUES (%s, %s, %s, %s)
#                         ON DUPLICATE KEY UPDATE 
#                             file_path = VALUES(file_path),
#                             status = 'pending',
#                             error_message = NULL,
#                             processed_at = NULL
#                         """,
#                         autocommit=True,
#                         parameters=(
#                             's3',
#                             'csv',
#                             file['Key'].split('/')[-1],
#                             f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
#                         )
#                     )
#                     file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
#                     registered_files.append({
#                         'file_key': file['Key'],
#                         'file_name': file['Key'].split('/')[-1],
#                         'source_system': 's3',
#                         'file_format': 'csv',
#                         'file_id': file_id,
#                         'last_modified': file['LastModified']
#                     })
#                 return registered_files

#             @task(task_id='process_files')
#             def process_files(registered_files):
#                 processed_files = []
#                 for file in registered_files:
#                     try:
#                         df = read_s3_file(
#                             CONFIG['s3_bucket'],
#                             file['file_key'],
#                             'csv'
#                         )
#                         data = validate_employee_data(df).to_dict('records')
#                         load_to_staging_table(
#                             CONFIG['mysql_conn_id'],
#                             data,
#                             {
#                                 'file_id': file['file_id'],
#                                 'file_name': file['file_name'],
#                                 'source_system': file['source_system']
#                             }
#                         )
#                         processed_files.append(file)
#                     except Exception as e:
#                         logger.error(f"Processing failed: {str(e)}")
#                         raise
#                 return processed_files

#             @task(task_id='move_to_main')
#             def move_to_main(processed_files):
#                 for file in processed_files:
#                     process_staging_to_main(
#                         CONFIG['mysql_conn_id'],
#                         {
#                             'file_id': file['file_id'],
#                             'file_name': file['file_name'],
#                             'source_system': file['source_system']
#                         }
#                     )
#                 return processed_files

#             @task(task_id='archive_files')
#             def archive_files(processed_files):
#                 if not processed_files:
#                     return True
                
#                 s3 = create_s3_client()
#                 timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#                 archive_key = f"{CONFIG['s3_paths']['csv']['archive']}csv_archive_{timestamp}.zip"
                
#                 ensure_s3_folder_exists(s3, CONFIG['s3_bucket'], CONFIG['s3_paths']['csv']['archive'], 'csv')
#                 source_keys = [file['file_key'] for file in processed_files]
                
#                 create_s3_zip_archive(
#                     s3,
#                     CONFIG['s3_bucket'],
#                     source_keys,
#                     CONFIG['s3_bucket'],
#                     archive_key,
#                     'csv'
#                 )
                
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 for file in processed_files:
#                     hook.run(
#                         """
#                         INSERT INTO dv_employee_load_audit 
#                         (file_name, source_system, records_processed, status)
#                         VALUES (%s, %s, %s, 'archived')
#                         """,
#                         autocommit=True,
#                         parameters=(file['file_name'], 's3', 1)
#                     )
#                 return True

#             @task(task_id='clean_landing_folder')
#             def clean_landing_folder(processed_files):
#                 if not processed_files:
#                     return True
                
#                 s3 = create_s3_client()
#                 objects_to_delete = [{'Key': file['file_key']} for file in processed_files]
#                 if objects_to_delete:
#                     s3.delete_objects(
#                         Bucket=CONFIG['s3_bucket'],
#                         Delete={'Objects': objects_to_delete}
#                     )
#                 return True

#             @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
#             def notify_completion():
#                 logger.info("Completed processing CSV files from S3")
#                 return True

#             file_list = list_files()
#             registered = register_files(file_list)
#             processed = process_files(registered)
#             moved = move_to_main(processed)
#             archived = archive_files(processed)
#             cleaned = clean_landing_folder(processed)
#             notify = notify_completion()
            
#             file_list >> registered >> processed >> moved >> archived >> cleaned >> notify

#         @task_group(group_id='s3_json')
#         def process_s3_json():
#             @task(task_id='list_files')
#             def list_files():
#                 s3 = create_s3_client()
#                 response = s3.list_objects_v2(
#                     Bucket=CONFIG['s3_bucket'],
#                     Prefix=CONFIG['s3_paths']['json']['landing']
#                 )
#                 return [obj for obj in response.get('Contents', []) 
#                        if not obj['Key'].endswith('/') 
#                        and os.path.splitext(obj['Key'])[1].lower() == '.json']

#             @task(task_id='register_files')
#             def register_files(files):
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 registered_files = []
#                 for file in files:
#                     file_id = hook.run(
#                         """
#                         INSERT INTO dv_stg_files 
#                         (source_system, file_format, file_name, file_path)
#                         VALUES (%s, %s, %s, %s)
#                         ON DUPLICATE KEY UPDATE 
#                             file_path = VALUES(file_path),
#                             status = 'pending',
#                             error_message = NULL,
#                             processed_at = NULL
#                         """,
#                         autocommit=True,
#                         parameters=(
#                             's3',
#                             'json',
#                             file['Key'].split('/')[-1],
#                             f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
#                         )
#                     )
#                     file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
#                     registered_files.append({
#                         'file_key': file['Key'],
#                         'file_name': file['Key'].split('/')[-1],
#                         'source_system': 's3',
#                         'file_format': 'json',
#                         'file_id': file_id,
#                         'last_modified': file['LastModified']
#                     })
#                 return registered_files

#             @task(task_id='process_files')
#             def process_files(registered_files):
#                 processed_files = []
#                 for file in registered_files:
#                     try:
#                         df = read_s3_file(
#                             CONFIG['s3_bucket'],
#                             file['file_key'],
#                             'json'
#                         )
#                         data = validate_employee_data(df).to_dict('records')
#                         load_to_staging_table(
#                             CONFIG['mysql_conn_id'],
#                             data,
#                             {
#                                 'file_id': file['file_id'],
#                                 'file_name': file['file_name'],
#                                 'source_system': file['source_system']
#                             }
#                         )
#                         processed_files.append(file)
#                     except Exception as e:
#                         logger.error(f"Processing failed: {str(e)}")
#                         raise
#                 return processed_files

#             @task(task_id='move_to_main')
#             def move_to_main(processed_files):
#                 for file in processed_files:
#                     process_staging_to_main(
#                         CONFIG['mysql_conn_id'],
#                         {
#                             'file_id': file['file_id'],
#                             'file_name': file['file_name'],
#                             'source_system': file['source_system']
#                         }
#                     )
#                 return processed_files

#             @task(task_id='archive_files')
#             def archive_files(processed_files):
#                 if not processed_files:
#                     return True
                
#                 s3 = create_s3_client()
#                 timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#                 archive_key = f"{CONFIG['s3_paths']['json']['archive']}json_archive_{timestamp}.zip"
                
#                 ensure_s3_folder_exists(s3, CONFIG['s3_bucket'], CONFIG['s3_paths']['json']['archive'], 'json')
#                 source_keys = [file['file_key'] for file in processed_files]
                
#                 create_s3_zip_archive(
#                     s3,
#                     CONFIG['s3_bucket'],
#                     source_keys,
#                     CONFIG['s3_bucket'],
#                     archive_key,
#                     'json'
#                 )
                
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 for file in processed_files:
#                     hook.run(
#                         """
#                         INSERT INTO dv_employee_load_audit 
#                         (file_name, source_system, records_processed, status)
#                         VALUES (%s, %s, %s, 'archived')
#                         """,
#                         autocommit=True,
#                         parameters=(file['file_name'], 's3', 1)
#                     )
#                 return True

#             @task(task_id='clean_landing_folder')
#             def clean_landing_folder(processed_files):
#                 if not processed_files:
#                     return True
                
#                 s3 = create_s3_client()
#                 objects_to_delete = [{'Key': file['file_key']} for file in processed_files]
#                 if objects_to_delete:
#                     s3.delete_objects(
#                         Bucket=CONFIG['s3_bucket'],
#                         Delete={'Objects': objects_to_delete}
#                     )
#                 return True

#             @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
#             def notify_completion():
#                 logger.info("Completed processing JSON files from S3")
#                 return True

#             file_list = list_files()
#             registered = register_files(file_list)
#             processed = process_files(registered)
#             moved = move_to_main(processed)
#             archived = archive_files(processed)
#             cleaned = clean_landing_folder(processed)
#             notify = notify_completion()
            
#             file_list >> registered >> processed >> moved >> archived >> cleaned >> notify

#         @task_group(group_id='s3_excel')
#         def process_s3_excel():
#             @task(task_id='list_files')
#             def list_files():
#                 s3 = create_s3_client()
#                 response = s3.list_objects_v2(
#                     Bucket=CONFIG['s3_bucket'],
#                     Prefix=CONFIG['s3_paths']['excel']['landing']
#                 )
#                 return [obj for obj in response.get('Contents', []) 
#                        if not obj['Key'].endswith('/') 
#                        and os.path.splitext(obj['Key'])[1].lower() in ['.xlsx', '.xls']]

#             @task(task_id='register_files')
#             def register_files(files):
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 registered_files = []
#                 for file in files:
#                     file_id = hook.run(
#                         """
#                         INSERT INTO dv_stg_files 
#                         (source_system, file_format, file_name, file_path)
#                         VALUES (%s, %s, %s, %s)
#                         ON DUPLICATE KEY UPDATE 
#                             file_path = VALUES(file_path),
#                             status = 'pending',
#                             error_message = NULL,
#                             processed_at = NULL
#                         """,
#                         autocommit=True,
#                         parameters=(
#                             's3',
#                             'excel',
#                             file['Key'].split('/')[-1],
#                             f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
#                         )
#                     )
#                     file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
#                     registered_files.append({
#                         'file_key': file['Key'],
#                         'file_name': file['Key'].split('/')[-1],
#                         'source_system': 's3',
#                         'file_format': 'excel',
#                         'file_id': file_id,
#                         'last_modified': file['LastModified']
#                     })
#                 return registered_files

#             @task(task_id='process_files')
#             def process_files(registered_files):
#                 processed_files = []
#                 for file in registered_files:
#                     try:
#                         df = read_s3_file(
#                             CONFIG['s3_bucket'],
#                             file['file_key'],
#                             'excel'
#                         )
#                         data = validate_employee_data(df).to_dict('records')
#                         load_to_staging_table(
#                             CONFIG['mysql_conn_id'],
#                             data,
#                             {
#                                 'file_id': file['file_id'],
#                                 'file_name': file['file_name'],
#                                 'source_system': file['source_system']
#                             }
#                         )
#                         processed_files.append(file)
#                     except Exception as e:
#                         logger.error(f"Processing failed: {str(e)}")
#                         raise
#                 return processed_files

#             @task(task_id='move_to_main')
#             def move_to_main(processed_files):
#                 for file in processed_files:
#                     process_staging_to_main(
#                         CONFIG['mysql_conn_id'],
#                         {
#                             'file_id': file['file_id'],
#                             'file_name': file['file_name'],
#                             'source_system': file['source_system']
#                         }
#                     )
#                 return processed_files

#             @task(task_id='archive_files')
#             def archive_files(processed_files):
#                 if not processed_files:
#                     return True
                
#                 s3 = create_s3_client()
#                 timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#                 archive_key = f"{CONFIG['s3_paths']['excel']['archive']}excel_archive_{timestamp}.zip"
                
#                 ensure_s3_folder_exists(s3, CONFIG['s3_bucket'], CONFIG['s3_paths']['excel']['archive'], 'excel')
#                 source_keys = [file['file_key'] for file in processed_files]
                
#                 create_s3_zip_archive(
#                     s3,
#                     CONFIG['s3_bucket'],
#                     source_keys,
#                     CONFIG['s3_bucket'],
#                     archive_key,
#                     'excel'
#                 )
                
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 for file in processed_files:
#                     hook.run(
#                         """
#                         INSERT INTO dv_employee_load_audit 
#                         (file_name, source_system, records_processed, status)
#                         VALUES (%s, %s, %s, 'archived')
#                         """,
#                         autocommit=True,
#                         parameters=(file['file_name'], 's3', 1)
#                     )
#                 return True

#             @task(task_id='clean_landing_folder')
#             def clean_landing_folder(processed_files):
#                 if not processed_files:
#                     return True
                
#                 s3 = create_s3_client()
#                 objects_to_delete = [{'Key': file['file_key']} for file in processed_files]
#                 if objects_to_delete:
#                     s3.delete_objects(
#                         Bucket=CONFIG['s3_bucket'],
#                         Delete={'Objects': objects_to_delete}
#                     )
#                 return True

#             @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
#             def notify_completion():
#                 logger.info("Completed processing Excel files from S3")
#                 return True

#             file_list = list_files()
#             registered = register_files(file_list)
#             processed = process_files(registered)
#             moved = move_to_main(processed)
#             archived = archive_files(processed)
#             cleaned = clean_landing_folder(processed)
#             notify = notify_completion()
            
#             file_list >> registered >> processed >> moved >> archived >> cleaned >> notify

#         # Execute all format processors in parallel
#         csv_processor = process_s3_csv()
#         json_processor = process_s3_json()
#         excel_processor = process_s3_excel()

#     @task_group(group_id='gdrive_processing')
#     def process_gdrive():
#         @task_group(group_id='gdrive_csv')
#         def process_gdrive_csv():
#             @task(task_id='list_files')
#             def list_files():
#                 return list_gdrive_files(
#                     CONFIG['gdrive_paths']['csv']['landing_folder_id'],
#                     CONFIG['gdrive_paths']['csv']['extensions'],
#                     'csv'
#                 )

#             @task(task_id='process_files')
#             def process_files(files):
#                 processed_files = []
#                 for file in files:
#                     try:
#                         data = process_employee_file(file['id'], file['name'], 'csv')
                        
#                         hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                         file_id = hook.run(
#                             """
#                             INSERT INTO dv_stg_files 
#                             (source_system, file_format, file_name, file_path, status)
#                             VALUES (%s, %s, %s, %s, 'staged')
#                             """,
#                             autocommit=True,
#                             parameters=(
#                                 'gdrive',
#                                 'csv',
#                                 file['name'],
#                                 f"gdrive:{file['id']}"
#                             )
#                         )
#                         file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                        
#                         load_to_staging_table(
#                             CONFIG['mysql_conn_id'],
#                             data,
#                             {
#                                 'file_id': file_id,
#                                 'file_name': file['name'],
#                                 'source_system': 'gdrive'
#                             }
#                         )
                        
#                         processed_files.append({
#                             'file_id': file['id'],
#                             'file_name': file['name'],
#                             'db_file_id': file_id
#                         })
#                     except Exception as e:
#                         logger.error(f"Processing failed: {str(e)}")
#                         raise
#                 return processed_files

#             @task(task_id='move_to_main')
#             def move_to_main(files):
#                 for file in files:
#                     process_staging_to_main(
#                         CONFIG['mysql_conn_id'],
#                         {
#                             'file_id': file['db_file_id'],
#                             'file_name': file['file_name'],
#                             'source_system': 'gdrive'
#                         }
#                     )
#                 return files

#             @task(task_id='create_and_archive_zip')
#             def create_and_archive_zip(files):
#                 try:
#                     if not files:
#                         logger.info("No files to archive")
#                         return True
                        
#                     service = get_gdrive_service()
                    
#                     # Try to create zip archive (may fail due to service account limitations)
#                     try:
#                         zip_id = create_gdrive_zip_archive(
#                             service,
#                             [file['file_id'] for file in files],
#                             [file['file_name'] for file in files],
#                             CONFIG['gdrive_paths']['csv']['landing_folder_id'],
#                             'csv'
#                         )
#                         move_gdrive_file(zip_id, CONFIG['gdrive_paths']['csv']['archive_folder_id'], 'csv')
#                         logger.info("Created and moved zip archive successfully")
#                     except HttpError as e:
#                         if 'storageQuotaExceeded' in str(e):
#                             logger.warning("Service account storage quota exceeded. Moving files individually instead of zipping.")
#                             for file in files:
#                                 move_gdrive_file(
#                                     file['file_id'],
#                                     CONFIG['gdrive_paths']['csv']['archive_folder_id'],
#                                     'csv'
#                                 )
#                             logger.info(f"Moved {len(files)} files individually to archive")
#                         else:
#                             raise
                    
#                     # Update audit log
#                     hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                     for file in files:
#                         hook.run(
#                             """
#                             INSERT INTO dv_employee_load_audit 
#                             (file_name, source_system, records_processed, status)
#                             VALUES (%s, %s, %s, 'archived')
#                             """,
#                             autocommit=True,
#                             parameters=(file['file_name'], 'gdrive', 1)
#                         )
                    
#                     return True
#                 except Exception as e:
#                     logger.error(f"Archive creation/move failed: {str(e)}")
#                     raise
                
#             # @task(task_id='create_and_archive_zip')
#             # def create_and_archive_zip(files):
#             #     if not files:
#             #         return True
                
#             #     service = get_gdrive_service()
#             #     zip_id = create_gdrive_zip_archive(
#             #         service,
#             #         [file['file_id'] for file in files],
#             #         [file['file_name'] for file in files],
#             #         CONFIG['gdrive_paths']['csv']['landing_folder_id'],
#             #         'csv'
#             #     )
#             #     move_gdrive_file(zip_id, CONFIG['gdrive_paths']['csv']['archive_folder_id'], 'csv')
                
#             #     hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#             #     for file in files:
#             #         hook.run(
#             #             """
#             #             INSERT INTO dv_employee_load_audit 
#             #             (file_name, source_system, records_processed, status)
#             #             VALUES (%s, %s, %s, 'archived')
#             #             """,
#             #             autocommit=True,
#             #             parameters=(file['file_name'], 'gdrive', 1)
#             #         )
#             #     return True

#             @task(task_id='clean_landing_folder')
#             def clean_landing_folder():
#                 service = get_gdrive_service()
#                 clean_gdrive_folder(service, CONFIG['gdrive_paths']['csv']['landing_folder_id'], 'csv')
#                 return True

#             @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
#             def notify_completion():
#                 logger.info("Completed processing CSV files from Google Drive")
#                 return True

#             file_list = list_files()
#             processed = process_files(file_list)
#             moved = move_to_main(processed)
#             archived = create_and_archive_zip(processed)
#             cleaned = clean_landing_folder()
#             notify = notify_completion()
            
#             file_list >> processed >> moved >> archived >> cleaned >> notify
#             # file_list >> processed >> archived >> cleaned >> notify


#         @task_group(group_id='gdrive_json')
#         def process_gdrive_json():
#             @task(task_id='list_files')
#             def list_files():
#                 return list_gdrive_files(
#                     CONFIG['gdrive_paths']['json']['landing_folder_id'],
#                     CONFIG['gdrive_paths']['json']['extensions'],
#                     'json'
#                 )

#             @task(task_id='process_files')
#             def process_files(files):
#                 processed_files = []
#                 for file in files:
#                     try:
#                         data = process_employee_file(file['id'], file['name'], 'json')
                        
#                         hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                         file_id = hook.run(
#                             """
#                             INSERT INTO dv_stg_files 
#                             (source_system, file_format, file_name, file_path, status)
#                             VALUES (%s, %s, %s, %s, 'staged')
#                             """,
#                             autocommit=True,
#                             parameters=(
#                                 'gdrive',
#                                 'json',
#                                 file['name'],
#                                 f"gdrive:{file['id']}"
#                             )
#                         )
#                         file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                        
#                         load_to_staging_table(
#                             CONFIG['mysql_conn_id'],
#                             data,
#                             {
#                                 'file_id': file_id,
#                                 'file_name': file['name'],
#                                 'source_system': 'gdrive'
#                             }
#                         )
                        
#                         processed_files.append({
#                             'file_id': file['id'],
#                             'file_name': file['name'],
#                             'db_file_id': file_id
#                         })
#                     except Exception as e:
#                         logger.error(f"Processing failed: {str(e)}")
#                         raise
#                 return processed_files

#             @task(task_id='move_to_main')
#             def move_to_main(files):
#                 for file in files:
#                     process_staging_to_main(
#                         CONFIG['mysql_conn_id'],
#                         {
#                             'file_id': file['db_file_id'],
#                             'file_name': file['file_name'],
#                             'source_system': 'gdrive'
#                         }
#                     )
#                 return files

#             # @task(task_id='create_and_archive_zip')
#             # def create_and_archive_zip(files):
#             #     if not files:
#             #         return True
                
#             #     service = get_gdrive_service()
#             #     zip_id = create_gdrive_zip_archive(
#             #         service,
#             #         [file['file_id'] for file in files],
#             #         [file['file_name'] for file in files],
#             #         CONFIG['gdrive_paths']['json']['landing_folder_id'],
#             #         'json'
#             #     )
#             #     move_gdrive_file(zip_id, CONFIG['gdrive_paths']['json']['archive_folder_id'], 'json')
                
#             #     hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#             #     for file in files:
#             #         hook.run(
#             #             """
#             #             INSERT INTO dv_employee_load_audit 
#             #             (file_name, source_system, records_processed, status)
#             #             VALUES (%s, %s, %s, 'archived')
#             #             """,
#             #             autocommit=True,
#             #             parameters=(file['file_name'], 'gdrive', 1)
#             #         )
#             #     return True

#             @task(task_id='create_and_archive_zip')
#             def create_and_archive_zip(files):
#                 try:
#                     if not files:
#                         logger.info("No files to archive")
#                         return True
                        
#                     service = get_gdrive_service()
                    
#                     # Try to create zip archive (may fail due to service account limitations)
#                     try:
#                         zip_id = create_gdrive_zip_archive(
#                             service,
#                             [file['file_id'] for file in files],
#                             [file['file_name'] for file in files],
#                             CONFIG['gdrive_paths']['json']['landing_folder_id'],
#                             'json'
#                         )
#                         move_gdrive_file(zip_id, CONFIG['gdrive_paths']['json']['archive_folder_id'], 'json')
#                         logger.info("Created and moved zip archive successfully")
#                     except HttpError as e:
#                         if 'storageQuotaExceeded' in str(e):
#                             logger.warning("Service account storage quota exceeded. Moving files individually instead of zipping.")
#                             for file in files:
#                                 move_gdrive_file(
#                                     file['file_id'],
#                                     CONFIG['gdrive_paths']['json']['archive_folder_id'],
#                                     'json'
#                                 )
#                             logger.info(f"Moved {len(files)} files individually to archive")
#                         else:
#                             raise
                    
#                     # Update audit log
#                     hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                     for file in files:
#                         hook.run(
#                             """
#                             INSERT INTO dv_employee_load_audit 
#                             (file_name, source_system, records_processed, status)
#                             VALUES (%s, %s, %s, 'archived')
#                             """,
#                             autocommit=True,
#                             parameters=(file['file_name'], 'gdrive', 1)
#                         )
                    
#                     return True
#                 except Exception as e:
#                     logger.error(f"Archive creation/move failed: {str(e)}")
#                     raise
                

#             @task(task_id='clean_landing_folder')
#             def clean_landing_folder():
#                 service = get_gdrive_service()
#                 clean_gdrive_folder(service, CONFIG['gdrive_paths']['json']['landing_folder_id'], 'json')
#                 return True

#             @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
#             def notify_completion():
#                 logger.info("Completed processing JSON files from Google Drive")
#                 return True

#             file_list = list_files()
#             processed = process_files(file_list)
#             moved = move_to_main(processed)
#             archived = create_and_archive_zip(processed)
#             cleaned = clean_landing_folder()
#             notify = notify_completion()
            
#             file_list >> processed >> moved >> archived >> cleaned >> notify

#         @task_group(group_id='gdrive_excel')
#         def process_gdrive_excel():
#             @task(task_id='list_files')
#             def list_files():
#                 return list_gdrive_files(
#                     CONFIG['gdrive_paths']['excel']['landing_folder_id'],
#                     CONFIG['gdrive_paths']['excel']['extensions'],
#                     'excel'
#                 )

#             @task(task_id='process_files')
#             def process_files(files):
#                 processed_files = []
#                 for file in files:
#                     try:
#                         data = process_employee_file(file['id'], file['name'], 'excel')
                        
#                         hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                         file_id = hook.run(
#                             """
#                             INSERT INTO dv_stg_files 
#                             (source_system, file_format, file_name, file_path, status)
#                             VALUES (%s, %s, %s, %s, 'staged')
#                             """,
#                             autocommit=True,
#                             parameters=(
#                                 'gdrive',
#                                 'excel',
#                                 file['name'],
#                                 f"gdrive:{file['id']}"
#                             )
#                         )
#                         file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                        
#                         load_to_staging_table(
#                             CONFIG['mysql_conn_id'],
#                             data,
#                             {
#                                 'file_id': file_id,
#                                 'file_name': file['name'],
#                                 'source_system': 'gdrive'
#                             }
#                         )
                        
#                         processed_files.append({
#                             'file_id': file['id'],
#                             'file_name': file['name'],
#                             'db_file_id': file_id
#                         })
#                     except Exception as e:
#                         logger.error(f"Processing failed: {str(e)}")
#                         raise
#                 return processed_files

#             @task(task_id='move_to_main')
#             def move_to_main(files):
#                 for file in files:
#                     process_staging_to_main(
#                         CONFIG['mysql_conn_id'],
#                         {
#                             'file_id': file['db_file_id'],
#                             'file_name': file['file_name'],
#                             'source_system': 'gdrive'
#                         }
#                     )
#                 return files

#             # @task(task_id='create_and_archive_zip')
#             # def create_and_archive_zip(files):
#                 if not files:
#                     return True
                
#                 service = get_gdrive_service()
#                 zip_id = create_gdrive_zip_archive(
#                     service,
#                     [file['file_id'] for file in files],
#                     [file['file_name'] for file in files],
#                     CONFIG['gdrive_paths']['excel']['landing_folder_id'],
#                     'excel'
#                 )
#                 move_gdrive_file(zip_id, CONFIG['gdrive_paths']['excel']['archive_folder_id'], 'excel')
                
#                 hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                 for file in files:
#                     hook.run(
#                         """
#                         INSERT INTO dv_employee_load_audit 
#                         (file_name, source_system, records_processed, status)
#                         VALUES (%s, %s, %s, 'archived')
#                         """,
#                         autocommit=True,
#                         parameters=(file['file_name'], 'gdrive', 1)
#                     )
#                 return True

#             @task(task_id='create_and_archive_zip')
#             def create_and_archive_zip(files):
#                 try:
#                     if not files:
#                         logger.info("No files to archive")
#                         return True
                        
#                     service = get_gdrive_service()
                    
#                     # Try to create zip archive (may fail due to service account limitations)
#                     try:
#                         zip_id = create_gdrive_zip_archive(
#                             service,
#                             [file['file_id'] for file in files],
#                             [file['file_name'] for file in files],
#                             CONFIG['gdrive_paths']['excel']['landing_folder_id'],
#                             'excel'
#                         )
#                         move_gdrive_file(zip_id, CONFIG['gdrive_paths']['excel']['archive_folder_id'], 'excel')
#                         logger.info("Created and moved zip archive successfully")
#                     except HttpError as e:
#                         if 'storageQuotaExceeded' in str(e):
#                             logger.warning("Service account storage quota exceeded. Moving files individually instead of zipping.")
#                             for file in files:
#                                 move_gdrive_file(
#                                     file['file_id'],
#                                     CONFIG['gdrive_paths']['excel']['archive_folder_id'],
#                                     'excel'
#                                 )
#                             logger.info(f"Moved {len(files)} files individually to archive")
#                         else:
#                             raise
                    
#                     # Update audit log
#                     hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#                     for file in files:
#                         hook.run(
#                             """
#                             INSERT INTO dv_employee_load_audit 
#                             (file_name, source_system, records_processed, status)
#                             VALUES (%s, %s, %s, 'archived')
#                             """,
#                             autocommit=True,
#                             parameters=(file['file_name'], 'gdrive', 1)
#                         )
                    
#                     return True
#                 except Exception as e:
#                     logger.error(f"Archive creation/move failed: {str(e)}")
#                     raise
                

#             @task(task_id='clean_landing_folder')
#             def clean_landing_folder():
#                 service = get_gdrive_service()
#                 clean_gdrive_folder(service, CONFIG['gdrive_paths']['excel']['landing_folder_id'], 'excel')
#                 return True

#             @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
#             def notify_completion():
#                 logger.info("Completed processing Excel files from Google Drive")
#                 return True

#             file_list = list_files()
#             processed = process_files(file_list)
#             moved = move_to_main(processed)
#             archived = create_and_archive_zip(processed)
#             cleaned = clean_landing_folder()
#             notify = notify_completion()
            
#             file_list >> processed >> moved >> archived >> cleaned >> notify

#         # Execute all format processors in parallel
#         csv_processor = process_gdrive_csv()
#         json_processor = process_gdrive_json()
#         excel_processor = process_gdrive_excel()

#     @task(task_id='clean_staging_tables')
#     def clean_staging_tables():
#         hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
#         hook.run("DELETE FROM dv_stg_employees")
#         hook.run("DELETE FROM dv_stg_files WHERE status = 'processed'")
#         return True

#     @task(task_id='send_final_notification')
#     def send_final_notification():
#         logger.info("All data processing completed successfully")
#         return True

#     @task(task_id='verify_gdrive_connection')
#     def verify_gdrive_connection():
#         service = get_gdrive_service()
#         service.files().list(pageSize=1, fields="files(id, name)").execute()
#         return True

#     @task(task_id='verify_gdrive_folders')
#     def verify_gdrive_folders():
#         service = get_gdrive_service()
#         for fmt, config in CONFIG['gdrive_paths'].items():
#             service.files().get(fileId=config['landing_folder_id'], fields='id,name').execute()
#             service.files().get(fileId=config['archive_folder_id'], fields='id,name').execute()
#         return True

#     # DAG dependencies
#     verify_conn = verify_gdrive_connection()
#     verify_folders = verify_gdrive_folders()
#     create_tables_task = create_tables
    
#     # Get the task groups
#     s3_processing_group = process_s3()
#     gdrive_processing_group = process_gdrive()
    
#     clean_staging = clean_staging_tables()
#     final_notification = send_final_notification()

#     # Set up dependencies
#     verify_conn >> verify_folders >> create_tables_task >> [s3_processing_group, gdrive_processing_group] >> clean_staging >> final_notification
#     # >> create_tables_task