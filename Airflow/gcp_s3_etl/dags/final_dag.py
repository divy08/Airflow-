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
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import os
import zipfile
import pandas as pd
import logging
import re
from airflow.exceptions import AirflowException
from io import BytesIO
from tenacity import retry, stop_after_attempt, wait_exponential

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

# Configuration
CONFIG = {
    'mysql_conn_id': "mysql_conn",
    's3_conn_id': "aws_s3_conn",
    's3_bucket': "fug-de-training",
    's3_paths': {
        'csv': {
            'landing': "dv_prac/landing_folder/csv_data/",
            'archive': "dv_prac/archive_folder/csv_data/",
            'extensions': ['.csv']
        },
        'json': {
            'landing': "dv_prac/landing_folder/json_data/",
            'archive': "dv_prac/archive_folder/json_data/",
            'extensions': ['.json']
        },
        'excel': {
            'landing': "dv_prac/landing_folder/excel_data/",
            'archive': "dv_prac/archive_folder/excel_data/",
            'extensions': ['.xlsx', '.xls']
        }
    },
    'gdrive_paths': {
        'csv': {
            'landing_folder_id': '13ZV6C8tT1P2sG8uu2bg19w3uI-aCSEIb',
            'archive_folder_id': '1z00QnPZJMeR1Kuz1Ba9f6gobT8YLOUni',
            'extensions': ['.csv'],
            'is_shared_drive': True
        },
        'json': {
            'landing_folder_id': '1yd3gaLu46SKrdZMWERWeNIsUua0yJQJ3',
            'archive_folder_id': '1oJ_H5pTvHcMMuXREE_TxbRGjgu7CTXGD',
            'extensions': ['.json'],
            'is_shared_drive': True
        },
        'excel': {
            'landing_folder_id': '1HP_Gtfk0S2iA_nh5ZPa7NLh6-Bt8HdZ2',
            'archive_folder_id': '1-mAg1EukJnEuL3QmZ4LSSuIha5PfWzDZ',
            'extensions': ['.xlsx', '.xls'],
            'is_shared_drive': True
        }
    },
    'chunk_size': 10000,
    'local_zip_path': '/tmp/airflow_zips'
}

# Google Drive Functions
def get_gdrive_service():
    """Create authenticated Google Drive service"""
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


def list_gdrive_files(folder_id, expected_extensions, format_type):
    """List files in Google Drive folder"""
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
        
        return filtered_files
    except Exception as e:
        logger.error(f"Failed to list {format_type} files: {str(e)}")
        raise AirflowException(f"Google Drive {format_type} listing error: {str(e)}")

def upload_zip_to_gdrive(zip_path, target_folder_id, format_type):
    """Upload locally created zip file to Google Drive"""
    try:
        service = get_gdrive_service()
        
        file_metadata = {
            'name': os.path.basename(zip_path),
            'parents': [target_folder_id],
            'mimeType': 'application/zip'
        }
        
        media = MediaFileUpload(zip_path, mimetype='application/zip')
        
        zip_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        logger.info(f"Uploaded {format_type} zip archive with ID: {zip_file['id']}")
        
        try:
            os.remove(zip_path)
        except Exception as e:
            logger.warning(f"Could not remove temporary zip file: {str(e)}")
        
        return zip_file['id']
    except Exception as e:
        logger.error(f"Failed to upload {format_type} zip archive: {str(e)}")
        raise AirflowException(f"Google Drive zip upload failed: {str(e)}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def move_gdrive_file(file_id, target_folder_id, format_type):
    """Move file in Google Drive"""
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
        
        logger.info(f"Moved file {file_id} to folder {target_folder_id}")
        return updated_file
    except Exception as e:
        logger.error(f"Failed to move {format_type} file: {str(e)}")
        raise AirflowException(f"Google Drive {format_type} file move failed: {str(e)}")

def clean_gdrive_folder(service, folder_id, format_type):
    """Clean Google Drive folder by moving files to archive"""
    try:
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            pageSize=100,
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            logger.info(f"No files found in {format_type} folder to clean")
            return True
            
        for file in files:
            move_gdrive_file(
                file['id'],
                CONFIG['gdrive_paths'][format_type]['archive_folder_id'],
                format_type
            )
        logger.info(f"Moved {len(files)} {format_type} files to archive")
                
        return True
    except Exception as e:
        logger.error(f"Failed to clean {format_type} Google Drive folder: {str(e)}")
        raise AirflowException(f"Google Drive {format_type} folder cleanup failed: {str(e)}")


def create_local_zip(file_ids, file_names, format_type, task_instance):
    """Create zip file locally and store file info in XCom"""
    try:
        os.makedirs(CONFIG['local_zip_path'], exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f"{format_type}_archive_{timestamp}.zip"
        zip_path = os.path.join(CONFIG['local_zip_path'], zip_filename)
        
        service = get_gdrive_service()
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_id, file_name in zip(file_ids, file_names):
                request = service.files().get_media(
                    fileId=file_id,
                    supportsAllDrives=True
                )
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                file_content.seek(0)
                
                zip_file.writestr(file_name, file_content.read())
                
                task_instance.xcom_push(
                    key=f"archived_{format_type}_files",
                    value={
                        'file_id': file_id,
                        'file_name': file_name,
                        'zip_path': zip_path
                    }
                )
        
        return zip_path
    except Exception as e:
        logger.error(f"Failed to create local zip for {format_type}: {str(e)}")
        raise AirflowException(f"Local zip creation failed: {str(e)}")



# Data Processing Functions
def read_gdrive_file(file_id, file_name, format_type):
    """Read file content from Google Drive"""
    try:
        service = get_gdrive_service()
        request = service.files().get_media(
            fileId=file_id,
            supportsAllDrives=True
        )
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        file_buffer.seek(0)
        
        if format_type == 'csv':
            df = pd.read_csv(file_buffer)
        elif format_type == 'json':
            df = pd.read_json(file_buffer)
        elif format_type == 'excel':
            df = pd.read_excel(file_buffer)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
            
        return df
    except Exception as e:
        logger.error(f"Failed to read {format_type} file: {str(e)}")
        raise AirflowException(f"{format_type} file reading error: {str(e)}")

def validate_employee_data(df):
    """Validate employee data structure"""
    required_cols = {'employee_id', 'firstname', 'lastname', 'salary', 'email'}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Missing columns: {missing}")
    
    df['employee_id'] = df['employee_id'].astype(str)
    if not df['employee_id'].str.match(r'^\d{4}$').all():
        raise ValueError("Invalid employee IDs")
    
    if (df['firstname'].str.len() > 20).any() or (df['lastname'].str.len() > 20).any():
        raise ValueError("Name length exceeds 20 chars")
    
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not df['email'].str.match(email_regex, na=False).all():
        raise ValueError("Invalid email formats")
    
    try:
        df['salary'] = pd.to_numeric(df['salary'])
    except ValueError as e:
        raise ValueError(f"Invalid salary values: {str(e)}")
    
    return df

def process_employee_file(file_id, file_name, format_type):
    """Process employee file with validation"""
    df = read_gdrive_file(file_id, file_name, format_type)
    return validate_employee_data(df).to_dict('records')

def load_to_staging_table(conn_id, data, file_info):
    """Load data to staging table"""
    hook = MySqlHook(mysql_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        values = [(r['employee_id'], r['firstname'], r['lastname'], 
                 float(r['salary']), r['email'], 
                 file_info['source_system'], file_info['file_name']) 
                 for r in data]
        
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
    except Exception as e:
        conn.rollback()
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
    except Exception as e:
        conn.rollback()
        hook.run("""
            UPDATE dv_stg_files 
            SET status = 'failed', error_message = %s 
            WHERE id = %s
        """, parameters=(str(e)[:500], file_info['file_id']))
        raise AirflowException(f"Processing error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# S3 Functions
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

def read_s3_file(bucket, key, format_type):
    """Read file content from S3"""
    try:
        s3 = create_s3_client()
        obj = s3.get_object(Bucket=bucket, Key=key)
        
        if format_type == 'csv':
            df = pd.read_csv(obj['Body'])
        elif format_type == 'json':
            df = pd.read_json(obj['Body'])
        elif format_type == 'excel':
            df = pd.read_excel(io.BytesIO(obj['Body'].read()))
        else:
            raise ValueError(f"Unsupported format: {format_type}")
            
        return df
    except Exception as e:
        logger.error(f"Failed to read {format_type} file: {str(e)}")
        raise AirflowException(f"S3 {format_type} file reading error: {str(e)}")

def create_s3_zip_archive(s3_client, source_bucket, source_keys, target_bucket, target_key, format_type):
    """Create zip archive in S3"""
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
        return True
    except Exception as e:
        logger.error(f"Failed to create {format_type} zip archive: {str(e)}")
        raise AirflowException(f"S3 {format_type} zip archive creation failed: {str(e)}")

def clean_s3_folder(s3_client, bucket, prefix, format_type):
    """Clean S3 folder"""
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
        return True
    except Exception as e:
        logger.error(f"Failed to clean {format_type} folder: {str(e)}")
        raise AirflowException(f"S3 {format_type} folder cleanup failed: {str(e)}")

def ensure_s3_folder_exists(s3_client, bucket, prefix, format_type):
    """Ensure S3 folder exists"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1
        )
        
        if 'Contents' not in response and 'CommonPrefixes' not in response:
            s3_client.put_object(Bucket=bucket, Key=f"{prefix}/")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure {format_type} folder exists: {str(e)}")
        raise AirflowException(f"S3 {format_type} folder creation failed: {str(e)}")

# DAG Definition
with DAG(
    'final_employee_data_processing',
    default_args=default_args,
    schedule=[LANDING_ZONE_DS],
    catchup=False,
    max_active_runs=1,
    tags=['employee_data', 'mysql', 's3', 'gdrive'],
    description='Process employee data from S3 and Google Drive to MySQL',
) as dag:

    # Database Setup
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

    # S3 Processing
    @task_group(group_id='s3_processing')
    def process_s3():
        @task_group(group_id='s3_csv')
        def process_s3_csv():
            @task(task_id='list_files')
            def list_files():
                s3 = create_s3_client()
                response = s3.list_objects_v2(
                    Bucket=CONFIG['s3_bucket'],
                    Prefix=CONFIG['s3_paths']['csv']['landing']
                )
                return [obj for obj in response.get('Contents', []) 
                       if not obj['Key'].endswith('/') 
                       and os.path.splitext(obj['Key'])[1].lower() == '.csv']

            @task(task_id='register_files')
            def register_files(files):
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
                            error_message = NULL,
                            processed_at = NULL
                        """,
                        autocommit=True,
                        parameters=(
                            's3',
                            'csv',
                            file['Key'].split('/')[-1],
                            f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
                        )
                    )
                    file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                    registered_files.append({
                        'file_key': file['Key'],
                        'file_name': file['Key'].split('/')[-1],
                        'source_system': 's3',
                        'file_format': 'csv',
                        'file_id': file_id,
                        'last_modified': file['LastModified']
                    })
                return registered_files

            @task(task_id='process_files')
            def process_files(registered_files):
                processed_files = []
                for file in registered_files:
                    try:
                        df = read_s3_file(
                            CONFIG['s3_bucket'],
                            file['file_key'],
                            'csv'
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
                        logger.error(f"Processing failed: {str(e)}")
                        raise
                return processed_files

            @task(task_id='move_to_main')
            def move_to_main(processed_files):
                for file in processed_files:
                    process_staging_to_main(
                        CONFIG['mysql_conn_id'],
                        {
                            'file_id': file['file_id'],
                            'file_name': file['file_name'],
                            'source_system': file['source_system']
                        }
                    )
                return processed_files

            @task(task_id='archive_files')
            def archive_files(processed_files):
                if not processed_files:
                    return True
                
                s3 = create_s3_client()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                archive_key = f"{CONFIG['s3_paths']['csv']['archive']}csv_archive_{timestamp}.zip"
                
                ensure_s3_folder_exists(s3, CONFIG['s3_bucket'], CONFIG['s3_paths']['csv']['archive'], 'csv')
                source_keys = [file['file_key'] for file in processed_files]
                
                create_s3_zip_archive(
                    s3,
                    CONFIG['s3_bucket'],
                    source_keys,
                    CONFIG['s3_bucket'],
                    archive_key,
                    'csv'
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
                return True

            @task(task_id='clean_landing_folder')
            def clean_landing_folder(processed_files):
                if not processed_files:
                    return True
                
                s3 = create_s3_client()
                objects_to_delete = [{'Key': file['file_key']} for file in processed_files]
                if objects_to_delete:
                    s3.delete_objects(
                        Bucket=CONFIG['s3_bucket'],
                        Delete={'Objects': objects_to_delete}
                    )
                return True

            @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
            def notify_completion():
                logger.info("Completed processing CSV files from S3")
                return True

            file_list = list_files()
            registered = register_files(file_list)
            processed = process_files(registered)
            moved = move_to_main(processed)
            archived = archive_files(processed)
            cleaned = clean_landing_folder(processed)
            notify = notify_completion()
            
            file_list >> registered >> processed >> moved >> archived >> cleaned >> notify

        @task_group(group_id='s3_json')
        def process_s3_json():
            @task(task_id='list_files')
            def list_files():
                s3 = create_s3_client()
                response = s3.list_objects_v2(
                    Bucket=CONFIG['s3_bucket'],
                    Prefix=CONFIG['s3_paths']['json']['landing']
                )
                return [obj for obj in response.get('Contents', []) 
                       if not obj['Key'].endswith('/') 
                       and os.path.splitext(obj['Key'])[1].lower() == '.json']

            @task(task_id='register_files')
            def register_files(files):
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
                            error_message = NULL,
                            processed_at = NULL
                        """,
                        autocommit=True,
                        parameters=(
                            's3',
                            'json',
                            file['Key'].split('/')[-1],
                            f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
                        )
                    )
                    file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                    registered_files.append({
                        'file_key': file['Key'],
                        'file_name': file['Key'].split('/')[-1],
                        'source_system': 's3',
                        'file_format': 'json',
                        'file_id': file_id,
                        'last_modified': file['LastModified']
                    })
                return registered_files

            @task(task_id='process_files')
            def process_files(registered_files):
                processed_files = []
                for file in registered_files:
                    try:
                        df = read_s3_file(
                            CONFIG['s3_bucket'],
                            file['file_key'],
                            'json'
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
                        logger.error(f"Processing failed: {str(e)}")
                        raise
                return processed_files

            @task(task_id='move_to_main')
            def move_to_main(processed_files):
                for file in processed_files:
                    process_staging_to_main(
                        CONFIG['mysql_conn_id'],
                        {
                            'file_id': file['file_id'],
                            'file_name': file['file_name'],
                            'source_system': file['source_system']
                        }
                    )
                return processed_files

            @task(task_id='archive_files')
            def archive_files(processed_files):
                if not processed_files:
                    return True
                
                s3 = create_s3_client()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                archive_key = f"{CONFIG['s3_paths']['json']['archive']}json_archive_{timestamp}.zip"
                
                ensure_s3_folder_exists(s3, CONFIG['s3_bucket'], CONFIG['s3_paths']['json']['archive'], 'json')
                source_keys = [file['file_key'] for file in processed_files]
                
                create_s3_zip_archive(
                    s3,
                    CONFIG['s3_bucket'],
                    source_keys,
                    CONFIG['s3_bucket'],
                    archive_key,
                    'json'
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
                return True

            @task(task_id='clean_landing_folder')
            def clean_landing_folder(processed_files):
                if not processed_files:
                    return True
                
                s3 = create_s3_client()
                objects_to_delete = [{'Key': file['file_key']} for file in processed_files]
                if objects_to_delete:
                    s3.delete_objects(
                        Bucket=CONFIG['s3_bucket'],
                        Delete={'Objects': objects_to_delete}
                    )
                return True

            @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
            def notify_completion():
                logger.info("Completed processing JSON files from S3")
                return True

            file_list = list_files()
            registered = register_files(file_list)
            processed = process_files(registered)
            moved = move_to_main(processed)
            archived = archive_files(processed)
            cleaned = clean_landing_folder(processed)
            notify = notify_completion()
            
            file_list >> registered >> processed >> moved >> archived >> cleaned >> notify

        @task_group(group_id='s3_excel')
        def process_s3_excel():
            @task(task_id='list_files')
            def list_files():
                s3 = create_s3_client()
                response = s3.list_objects_v2(
                    Bucket=CONFIG['s3_bucket'],
                    Prefix=CONFIG['s3_paths']['excel']['landing']
                )
                return [obj for obj in response.get('Contents', []) 
                       if not obj['Key'].endswith('/') 
                       and os.path.splitext(obj['Key'])[1].lower() in ['.xlsx', '.xls']]

            @task(task_id='register_files')
            def register_files(files):
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
                            error_message = NULL,
                            processed_at = NULL
                        """,
                        autocommit=True,
                        parameters=(
                            's3',
                            'excel',
                            file['Key'].split('/')[-1],
                            f"s3://{CONFIG['s3_bucket']}/{file['Key']}"
                        )
                    )
                    file_id = hook.get_first("SELECT LAST_INSERT_ID()")[0]
                    registered_files.append({
                        'file_key': file['Key'],
                        'file_name': file['Key'].split('/')[-1],
                        'source_system': 's3',
                        'file_format': 'excel',
                        'file_id': file_id,
                        'last_modified': file['LastModified']
                    })
                return registered_files

            @task(task_id='process_files')
            def process_files(registered_files):
                processed_files = []
                for file in registered_files:
                    try:
                        df = read_s3_file(
                            CONFIG['s3_bucket'],
                            file['file_key'],
                            'excel'
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
                        logger.error(f"Processing failed: {str(e)}")
                        raise
                return processed_files

            @task(task_id='move_to_main')
            def move_to_main(processed_files):
                for file in processed_files:
                    process_staging_to_main(
                        CONFIG['mysql_conn_id'],
                        {
                            'file_id': file['file_id'],
                            'file_name': file['file_name'],
                            'source_system': file['source_system']
                        }
                    )
                return processed_files

            @task(task_id='archive_files')
            def archive_files(processed_files):
                if not processed_files:
                    return True
                
                s3 = create_s3_client()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                archive_key = f"{CONFIG['s3_paths']['excel']['archive']}excel_archive_{timestamp}.zip"
                
                ensure_s3_folder_exists(s3, CONFIG['s3_bucket'], CONFIG['s3_paths']['excel']['archive'], 'excel')
                source_keys = [file['file_key'] for file in processed_files]
                
                create_s3_zip_archive(
                    s3,
                    CONFIG['s3_bucket'],
                    source_keys,
                    CONFIG['s3_bucket'],
                    archive_key,
                    'excel'
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
                return True

            @task(task_id='clean_landing_folder')
            def clean_landing_folder(processed_files):
                if not processed_files:
                    return True
                
                s3 = create_s3_client()
                objects_to_delete = [{'Key': file['file_key']} for file in processed_files]
                if objects_to_delete:
                    s3.delete_objects(
                        Bucket=CONFIG['s3_bucket'],
                        Delete={'Objects': objects_to_delete}
                    )
                return True

            @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
            def notify_completion():
                logger.info("Completed processing Excel files from S3")
                return True

            file_list = list_files()
            registered = register_files(file_list)
            processed = process_files(registered)
            moved = move_to_main(processed)
            archived = archive_files(processed)
            cleaned = clean_landing_folder(processed)
            notify = notify_completion()
            
            file_list >> registered >> processed >> moved >> archived >> cleaned >> notify

        # Execute all format processors in parallel
        csv_processor = process_s3_csv()
        json_processor = process_s3_json()
        excel_processor = process_s3_excel()

    # Google Drive Processing
    @task_group(group_id='gdrive_processing')
    def process_gdrive():
        @task_group(group_id='gdrive_json')
        def process_gdrive_json():
            @task(task_id='list_files')
            def list_files():
                return list_gdrive_files(
                    CONFIG['gdrive_paths']['json']['landing_folder_id'],
                    CONFIG['gdrive_paths']['json']['extensions'],
                    'json'
                )

            @task(task_id='process_files')
            def process_files(files):
                processed_files = []
                for file in files:
                    try:
                        data = process_employee_file(file['id'], file['name'], 'json')
                        
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
                                'json',
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
                        logger.error(f"Processing failed: {str(e)}")
                        raise
                return processed_files

            @task(task_id='move_to_main')
            def move_to_main(files):
                for file in files:
                    process_staging_to_main(
                        CONFIG['mysql_conn_id'],
                        {
                            'file_id': file['db_file_id'],
                            'file_name': file['file_name'],
                            'source_system': 'gdrive'
                        }
                    )
                return files

            @task(task_id='create_local_zip')
            def create_local_zip_task(files, **kwargs):
                if not files:
                    logger.info("No files to archive")
                    return None
                
                task_instance = kwargs['ti']
                file_ids = [file['file_id'] for file in files]
                file_names = [file['file_name'] for file in files]
                
                zip_path = create_local_zip(
                    file_ids,
                    file_names,
                    'json',
                    task_instance
                )
                
                return zip_path

            @task(task_id='upload_zip_to_gdrive')
            def upload_zip_to_gdrive_task(**kwargs):
                task_instance = kwargs['ti']
                zip_path = task_instance.xcom_pull(task_ids='gdrive_processing.gdrive_json.create_local_zip')
                
                if not zip_path:
                    return True
                
                upload_zip_to_gdrive(
                    zip_path,
                    CONFIG['gdrive_paths']['json']['archive_folder_id'],
                    'json'
                )
                
                # Update audit log
                files = task_instance.xcom_pull(
                    task_ids='gdrive_processing.gdrive_json.create_local_zip',
                    key='archived_json_files'
                )
                
                if files:
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
                
                return True

            @task(task_id='clean_landing_folder')
            def clean_landing_folder():
                service = get_gdrive_service()
                clean_gdrive_folder(service, CONFIG['gdrive_paths']['json']['landing_folder_id'], 'json')
                return True

            @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
            def notify_completion():
                logger.info("Completed processing JSON files from Google Drive")
                return True

            file_list = list_files()
            processed = process_files(file_list)
            moved = move_to_main(processed)
            zip_path = create_local_zip_task(processed)
            uploaded = upload_zip_to_gdrive_task()
            cleaned = clean_landing_folder()
            notify = notify_completion()
            
            file_list >> processed >> moved >> zip_path >> uploaded >> cleaned >> notify
            # file_list >> processed >> moved >> cleaned >> notify

        @task_group(group_id='gdrive_csv')
        def process_gdrive_csv():
            @task(task_id='list_files')
            def list_files():
                return list_gdrive_files(
                    CONFIG['gdrive_paths']['csv']['landing_folder_id'],
                    CONFIG['gdrive_paths']['csv']['extensions'],
                    'csv'
                )

            @task(task_id='process_files')
            def process_files(files):
                processed_files = []
                for file in files:
                    try:
                        data = process_employee_file(file['id'], file['name'], 'csv')
                        
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
                                'csv',
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
                        logger.error(f"Processing failed: {str(e)}")
                        raise
                return processed_files

            @task(task_id='move_to_main')
            def move_to_main(files):
                for file in files:
                    process_staging_to_main(
                        CONFIG['mysql_conn_id'],
                        {
                            'file_id': file['db_file_id'],
                            'file_name': file['file_name'],
                            'source_system': 'gdrive'
                        }
                    )
                return files

            @task(task_id='create_local_zip')
            def create_local_zip_task(files, **kwargs):
                if not files:
                    logger.info("No files to archive")
                    return None
                
                task_instance = kwargs['ti']
                file_ids = [file['file_id'] for file in files]
                file_names = [file['file_name'] for file in files]
                
                zip_path = create_local_zip(
                    file_ids,
                    file_names,
                    'csv',
                    task_instance
                )
                
                return zip_path

            @task(task_id='upload_zip_to_gdrive')
            def upload_zip_to_gdrive_task(**kwargs):
                task_instance = kwargs['ti']
                zip_path = task_instance.xcom_pull(task_ids='gdrive_processing.gdrive_csv.create_local_zip')
                
                if not zip_path:
                    return True
                
                upload_zip_to_gdrive(
                    zip_path,
                    CONFIG['gdrive_paths']['csv']['archive_folder_id'],
                    'csv'
                )
                
                # Update audit log
                files = task_instance.xcom_pull(
                    task_ids='gdrive_processing.gdrive_csv.create_local_zip',
                    key='archived_csv_files'
                )
                
                if files:
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
                
                return True

            @task(task_id='clean_landing_folder')
            def clean_landing_folder():
                service = get_gdrive_service()
                clean_gdrive_folder(service, CONFIG['gdrive_paths']['csv']['landing_folder_id'], 'csv')
                return True

            @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
            def notify_completion():
                logger.info("Completed processing CSV files from Google Drive")
                return True

            file_list = list_files()
            processed = process_files(file_list)
            moved = move_to_main(processed)
            zip_path = create_local_zip_task(processed)
            uploaded = upload_zip_to_gdrive_task()
            cleaned = clean_landing_folder()
            notify = notify_completion()
            
            file_list >> processed >> moved >> zip_path >> uploaded >> cleaned >> notify

        @task_group(group_id='gdrive_excel')
        def process_gdrive_excel():
            @task(task_id='list_files')
            def list_files():
                return list_gdrive_files(
                    CONFIG['gdrive_paths']['excel']['landing_folder_id'],
                    CONFIG['gdrive_paths']['excel']['extensions'],
                    'excel'
                )

            @task(task_id='process_files')
            def process_files(files):
                processed_files = []
                for file in files:
                    try:
                        data = process_employee_file(file['id'], file['name'], 'excel')
                        
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
                                'excel',
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
                        logger.error(f"Processing failed: {str(e)}")
                        raise
                return processed_files

            @task(task_id='move_to_main')
            def move_to_main(files):
                for file in files:
                    process_staging_to_main(
                        CONFIG['mysql_conn_id'],
                        {
                            'file_id': file['db_file_id'],
                            'file_name': file['file_name'],
                            'source_system': 'gdrive'
                        }
                    )
                return files

            @task(task_id='create_local_zip')
            def create_local_zip_task(files, **kwargs):
                if not files:
                    logger.info("No files to archive")
                    return None
                
                task_instance = kwargs['ti']
                file_ids = [file['file_id'] for file in files]
                file_names = [file['file_name'] for file in files]
                
                zip_path = create_local_zip(
                    file_ids,
                    file_names,
                    'excel',
                    task_instance
                )
                
                return zip_path

            @task(task_id='upload_zip_to_gdrive')
            def upload_zip_to_gdrive_task(**kwargs):
                task_instance = kwargs['ti']
                zip_path = task_instance.xcom_pull(task_ids='gdrive_processing.gdrive_excel.create_local_zip')
                
                if not zip_path:
                    return True
                
                upload_zip_to_gdrive(
                    zip_path,
                    CONFIG['gdrive_paths']['excel']['archive_folder_id'],
                    'excel'
                )
                
                # Update audit log
                files = task_instance.xcom_pull(
                    task_ids='gdrive_processing.gdrive_excel.create_local_zip',
                    key='archived_excel_files'
                )
                
                if files:
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
                
                return True

            @task(task_id='clean_landing_folder')
            def clean_landing_folder():
                service = get_gdrive_service()
                clean_gdrive_folder(service, CONFIG['gdrive_paths']['excel']['landing_folder_id'], 'excel')
                return True

            @task(task_id='notify_completion', outlets=[ARCHIVE_DS])
            def notify_completion():
                logger.info("Completed processing Excel files from Google Drive")
                return True

            file_list = list_files()
            processed = process_files(file_list)
            moved = move_to_main(processed)
            zip_path = create_local_zip_task(processed)
            uploaded = upload_zip_to_gdrive_task()
            cleaned = clean_landing_folder()
            notify = notify_completion()
            
            file_list >> processed >> moved >> zip_path >> uploaded >> cleaned >> notify     

        # Execute all format processors in parallel
        csv_processor = process_gdrive_csv()
        json_processor = process_gdrive_json()
        excel_processor = process_gdrive_excel()

    # Final Tasks
    @task(task_id='clean_staging_tables')
    def clean_staging_tables():
        hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
        hook.run("DELETE FROM dv_stg_employees")
        hook.run("DELETE FROM dv_stg_files WHERE status = 'processed'")
        return True

    @task(task_id='send_final_notification')
    def send_final_notification():
        logger.info("All data processing completed successfully")
        return True

    @task(task_id='verify_gdrive_connection')
    def verify_gdrive_connection():
        service = get_gdrive_service()
        service.files().list(pageSize=1, fields="files(id, name)").execute()
        return True

    @task(task_id='verify_gdrive_folders')
    def verify_gdrive_folders():
        service = get_gdrive_service()
        for fmt, config in CONFIG['gdrive_paths'].items():
            try:
                if config.get('is_shared_drive', False):
                    # For Shared Drives, use the drives().get() endpoint
                    service.drives().get(driveId=config['landing_folder_id']).execute()
                    service.drives().get(driveId=config['archive_folder_id']).execute()
                else:
                    # Regular folders
                    service.files().get(fileId=config['landing_folder_id'], fields='id,name').execute()
                    service.files().get(fileId=config['archive_folder_id'], fields='id,name').execute()
            except HttpError as e:
                if 'driveId' in str(e):
                    # Fall back to regular file check if driveId lookup fails
                    service.files().get(fileId=config['landing_folder_id'], fields='id,name').execute()
                    service.files().get(fileId=config['archive_folder_id'], fields='id,name').execute()
                else:
                    raise
        return True

    # DAG Dependencies
    verify_conn = verify_gdrive_connection()
    verify_folders = verify_gdrive_folders()
    create_tables_task = create_tables
    
    s3_processing_group = process_s3()
    gdrive_processing_group = process_gdrive()
    
    clean_staging = clean_staging_tables()
    final_notification = send_final_notification()

    verify_conn >> verify_folders >> create_tables_task >> [s3_processing_group, gdrive_processing_group] >> clean_staging >> final_notification