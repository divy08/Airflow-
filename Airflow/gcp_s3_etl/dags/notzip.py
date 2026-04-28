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

def get_gdrive_service():
    """Create authenticated Google Drive service using Airflow Variable"""
    try:
        creds = Variable.get("gcp_config", deserialize_json=True)
        if not creds:
            raise AirflowException("GCP configuration not found in Airflow Variables")
        
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
            file_ext = os.path.splitext(file['name'])[1].lower()
            if not file_ext and 'fileExtension' in file:
                file_ext = f".{file['fileExtension'].lower()}"
            
            if any(file_ext == ext.lower() for ext in expected_extensions):
                filtered_files.append(file)
            else:
                logger.warning(f"Skipping file {file['name']} with extension {file_ext}")
        
        return filtered_files
    except Exception as e:
        logger.error(f"Failed to list files in folder {folder_id}: {str(e)}")
        raise AirflowException(f"Google Drive listing error: {str(e)}")

def create_gdrive_zip_archive(service, file_ids, file_names, target_folder_id):
    """Create a zip archive in Google Drive from multiple files"""
    try:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for file_id, file_name in zip(file_ids, file_names):
                request = service.files().get_media(fileId=file_id)
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                file_content.seek(0)
                zip_file.writestr(file_name, file_content.read())
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_name = f"archive_{timestamp}.zip"
        
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
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute()
        files = results.get('files', [])
        
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
        file = service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()
        
        previous_parents = ",".join(file.get('parents', []))
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
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info(f"Downloaded {int(status.progress() * 100)}% of {file_name}")
        
        file_buffer.seek(0)
        
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
        df['employee_id'] = df['employee_id'].astype(str)
        invalid_ids = df[~df['employee_id'].str.match(r'^\d{4}$')]
        if not invalid_ids.empty:
            raise ValueError(f"Invalid employee IDs: {invalid_ids['employee_id'].tolist()}")
            
        df['firstname'] = df['firstname'].str.strip()
        df['lastname'] = df['lastname'].str.strip()
        
        name_length_issues = df[
            (df['firstname'].str.len() > 20) | 
            (df['lastname'].str.len() > 20)
        ]
        if not name_length_issues.empty:
            raise ValueError(f"Name length exceeds 20 chars for records: {name_length_issues.index.tolist()}")
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid_emails = df[~df['email'].str.match(email_regex, na=False)]
        if not invalid_emails.empty:
            raise ValueError(f"Invalid email formats: {invalid_emails['email'].tolist()}")
        
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
        df = read_gdrive_file(file_id, file_name, file_format)
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
        
        cursor.executemany("""
            INSERT INTO dv_stg_employees 
            (employee_id, firstname, lastname, salary, email, source_system, file_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, values)
        
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
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dv_stg_employees 
            WHERE source_system = %s AND file_name = %s
        """, (file_info['source_system'], file_info['file_name']))
        record_count = cursor.fetchone()[0]
        
        if record_count == 0:
            logger.warning(f"No records found for {file_info['file_name']}")
            return
        
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
        
        cursor.execute("""
            INSERT INTO dv_employee_load_audit 
            (file_name, source_system, records_processed, status)
            VALUES (%s, %s, %s, 'processed')
        """, (file_info['file_name'], file_info['source_system'], record_count))
        
        cursor.execute("""
            DELETE FROM dv_stg_employees
            WHERE source_system = %s AND file_name = %s
        """, (file_info['source_system'], file_info['file_name']))
        
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
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for source_key in source_keys:
                file_name = source_key.split('/')[-1]
                obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
                file_content = obj['Body'].read()
                zip_file.writestr(file_name, file_content)
        
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
        objects_to_delete = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
        
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
    'no_zip_employee_data_processing',
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
                            df = read_s3_file(
                                CONFIG['s3_bucket'],
                                file['file_key'],
                                file_format
                            )
                            data = validate_employee_data(df).to_dict('records')
                            
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
                        source_keys = [file['file_key'] for file in processed_files]
                        archive_folder = f"{CONFIG['s3_archive_prefix']}/{format_config['folder']}"
                        archive_key = f"{archive_folder}/archive_{timestamp}.zip"
                        
                        create_s3_zip_archive(
                            s3,
                            CONFIG['s3_bucket'],
                            source_keys,
                            CONFIG['s3_bucket'],
                            archive_key
                        )
                        
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
        for file_format, format_config in CONFIG['file_formats'].items():
            @task_group(group_id=f'gdrive_{file_format}')
            def process_gdrive_format():
                @task(task_id=f'list_files')
                def list_files():
                    try:
                        logger.info(f"Processing {file_format} files from folder {format_config['landing_folder_id']}")
                        files = list_gdrive_files(
                            format_config['landing_folder_id'],
                            format_config['extensions']
                        )
                        
                        if not files:
                            logger.warning(f"No {file_format} files found in folder {format_config['landing_folder_id']}")
                            return []
                        
                        logger.info(f"Found {len(files)} {file_format} files")
                        return files
                    except Exception as e:
                        logger.error(f"Failed to list {file_format} files: {str(e)}")
                        raise AirflowException(f"File listing error: {str(e)}")

                @task(task_id=f'process_files')
                def process_files(files):
                    processed_files = []
                    for file in files:
                        try:
                            file_ext = os.path.splitext(file['name'])[1].lower()
                            if not any(file_ext == ext.lower() for ext in format_config['extensions']):
                                logger.error(f"File {file['name']} has invalid extension {file_ext}")
                                continue
                                
                            data = process_employee_file(file['id'], file['name'], file_format)
                            
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
                            file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                            
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
                        create_gdrive_zip_archive(
                            service,
                            [file['file_id'] for file in files],
                            [file['file_name'] for file in files],
                            format_config['archive_folder_id']
                        )
                        
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
                        logger.info("Cleaned Google Drive landing folder")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to clean landing folder: {str(e)}")
                        raise

                @task(task_id=f'notify_completion', outlets=[ARCHIVE_DS])
                def notify_completion():
                    logger.info(f"Completed processing {file_format} files from Google Drive")
                    return True

                file_list = list_files()
                processed = process_files(file_list)
                moved = move_to_main(processed)
                archived = archive_files(processed)
                cleaned = clean_landing_folder(processed)
                notify = notify_completion()
                
                file_list >> processed >> moved >> archived >> cleaned >> notify
            
            process_gdrive_format()

    @task(task_id='clean_staging_tables')
    def clean_staging_tables():
        try:
            hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
            deleted_count = hook.run("DELETE FROM dv_stg_employees")
            logger.info(f"Cleaned stg_employees table - removed {deleted_count} records")
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
        try:
            service = get_gdrive_service()
            results = service.files().list(
                pageSize=1,
                fields="files(id, name)"
            ).execute()
            logger.info("Google Drive connection successful")
            return True
        except Exception as e:
            logger.error("Google Drive connection test failed!")
            raise AirflowException("Google Drive connection verification failed")

    verify_conn = verify_gdrive_connection()
    create_tables_task = create_tables
    processing = [process_s3(), process_gdrive()]
    clean_staging = clean_staging_tables()
    final_notification = send_final_notification()

    verify_conn >> create_tables_task >> processing
    processing >> clean_staging >> final_notification