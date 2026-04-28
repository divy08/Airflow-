import logging
import uuid
import re
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
from config import S3_BUCKET, S3_PREFIX, STAGING_TABLE, FINAL_TABLE, ERROR_REPORT_FILENAME, MYSQL_CONN_ID

def generate_unique_billing_code(base_code, existing_codes, firstname, lastname, emp_id, max_attempts=100):
    """Generate unique billing code with conflict resolution"""
    if base_code not in existing_codes:
        return base_code
    
    patterns = [
        {'serial_range': (1, 9), 'firstname_chars': 2, 'lastname_chars': 3},
        {'serial_range': (10, 99), 'firstname_chars': 2, 'lastname_chars': 2},
        {'serial_range': (100, 999), 'firstname_chars': 2, 'lastname_chars': 1},
        {'serial_range': (1000, 9999), 'firstname_chars': 2, 'lastname_chars': 0},
        {'serial_range': (10000, 99999), 'firstname_chars': 1, 'lastname_chars': 0},
        {'serial_range': (100000, 999999), 'firstname_chars': 0, 'lastname_chars': 0},
        {'serial_range': (1000000, 9999999), 'pattern': lambda i, fn, ln, eid: f"{i}{eid}"},
        {'serial_range': (10000000, 99999999), 'pattern': lambda i, fn, ln, eid: f"X{i}{eid}"}
    ]

    for pattern in patterns:
        start, end = pattern['serial_range']
        for i in range(start, end + 1):
            if 'pattern' in pattern:
                candidate = pattern['pattern'](i, firstname, lastname, emp_id)
            else:
                fn_part = firstname[:pattern['firstname_chars']] if pattern['firstname_chars'] > 0 else ""
                ln_part = lastname[:pattern['lastname_chars']] if pattern['lastname_chars'] > 0 else ""
                candidate = f"{i}{fn_part}{ln_part}{emp_id}"
            
            if candidate not in existing_codes:
                return candidate
    
    # Fallback if all patterns exhausted
    for _ in range(max_attempts):
        candidate = f"UID{uuid.uuid4().hex[:6]}"
        if candidate not in existing_codes:
            return candidate
    
    raise ValueError(f"Failed to generate unique billing code after {max_attempts} attempts")

def extract_data_from_s3(**kwargs):
    """Extract CSV data from S3 bucket"""
    try:
        s3_hook = S3Hook(aws_conn_id='aws_cred')
        files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
        
        if not files:
            raise AirflowException(f"No files found in s3://{S3_BUCKET}/{S3_PREFIX}")
        
        csv_file = next((f for f in files if f.lower().endswith('.csv')), None)
        if not csv_file:
            raise AirflowException(f"No CSV files found in s3://{S3_BUCKET}/{S3_PREFIX}")
        
        file_obj = s3_hook.get_key(bucket_name=S3_BUCKET, key=csv_file)
        csv_content = file_obj.get()['Body'].read().decode('utf-8')
        
        kwargs['ti'].xcom_push(key='csv_content', value=csv_content)
        kwargs['ti'].xcom_push(key='source_filename', value=csv_file)
        logging.info(f"Extracted {csv_file} from S3")
        return csv_content
    except Exception as e:
        logging.error(f"S3 extraction failed: {str(e)}")
        raise AirflowException(f"S3 extraction failed: {str(e)}")

def validate_and_stage_records(**kwargs):
    """Validate records and stage in MySQL with duplicate email handling"""
    try:
        ti = kwargs['ti']
        csv_content = ti.xcom_pull(task_ids='extract_data_from_s3', key='csv_content')
        source_file = ti.xcom_pull(task_ids='extract_data_from_s3', key='source_filename')
        
        # Read and clean data
        df = pd.read_csv(
            StringIO(csv_content),
            keep_default_na=True,
            na_values=['', 'NA', 'N/A', 'null', 'NULL', 'NaN', 'nan'],
            dtype={'employee_id': 'Int64'}
        )
        df = df.replace([np.nan, None], None)
        df['salary'] = pd.to_numeric(df['salary'], errors='coerce').fillna(0)
        
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # Get all existing billing codes from final table
        cursor.execute(f"SELECT billing_code FROM {FINAL_TABLE}")
        existing_billing_codes = {row[0] for row in cursor.fetchall()}
        
        # Get existing emails from staging table to avoid duplicates
        cursor.execute(f"SELECT email FROM {STAGING_TABLE}")
        existing_staging_emails = {row[0] for row in cursor.fetchall()}
        
        # Clear staging table if needed
        if existing_staging_emails:
            cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
            existing_staging_emails = set()
        
        valid_records = []
        invalid_records = []
        duplicate_records = []
        staging_inserts = []
        
        for _, row in df.iterrows():
            row_dict = row.where(pd.notnull(row), None).to_dict()
            email = str(row_dict.get('email', ''))
            
            
            
            # Validate fields
            errors = []
            employee_id = row_dict.get('employee_id')
            if employee_id is None or not (isinstance(employee_id, int) and 1000 <= employee_id <= 9999):
                errors.append("Invalid employee_id (must be 4-digit integer)")
            
            firstname = str(row_dict.get('firstname', '')).strip()
            lastname = str(row_dict.get('lastname', '')).strip()
            if not firstname or len(firstname) > 20:
                errors.append("Invalid firstname (required, max 20 chars)")
            if not lastname or len(lastname) > 20:
                errors.append("Invalid lastname (required, max 20 chars)")
            
            salary = row_dict.get('salary', 0)
            if not isinstance(salary, (int, float)) or salary < 0:
                errors.append("Invalid salary (must be non-negative number)")
            
            if not email or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
                errors.append("Invalid email format")

            # Check for duplicate email in staging
            if email in existing_staging_emails:
                duplicate_records.append({
                    **row_dict,
                    'source_file': source_file,
                    'error_reason': f'Duplicate email in current batch: {email}'
                })
                continue
            if errors:
                invalid_records.append({
                    **row_dict,
                    'source_file': source_file,
                    'error_reason': '; '.join(errors)
                })
                continue
            
            # Generate billing code
            emp_id = str(employee_id)
            base_code = f"{firstname[:2].upper()}{lastname[:4].upper()}{emp_id}"
            
            try:
                billing_code = generate_unique_billing_code(
                    base_code=base_code,
                    existing_codes=existing_billing_codes,
                    firstname=firstname.upper(),
                    lastname=lastname.upper(),
                    emp_id=emp_id
                )
            except Exception as e:
                invalid_records.append({
                    **row_dict,
                    'source_file': source_file,
                    'error_reason': f"Billing code generation failed: {str(e)}"
                })
                continue
            
            # Prepare record for staging
            resource_id = billing_code[:6]
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            staging_inserts.append((
                employee_id,
                firstname,
                lastname,
                float(salary),
                email,
                billing_code,
                resource_id,
                created_at
            ))
            
            valid_records.append(row_dict)
            existing_staging_emails.add(email)
            existing_billing_codes.add(billing_code)
        
        # Insert valid records into staging with error handling
        if staging_inserts:
            try:
                cursor.executemany(
                    f"""
                    INSERT INTO {STAGING_TABLE} 
                    (employee_id, firstname, lastname, salary, email, billing_code, resource_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    staging_inserts
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise AirflowException(f"Failed to insert records into staging: {str(e)}")
        
        # Generate error report if needed
        if invalid_records:
            error_df = pd.DataFrame(invalid_records)
            error_df.to_excel(ERROR_REPORT_FILENAME, index=False)
            ti.xcom_push(key='error_file', value=ERROR_REPORT_FILENAME)
            ti.xcom_push(key='error_count', value=len(invalid_records) )
        
        cursor.close()
        conn.close()
        
        logging.info(f"Processed: {len(valid_records)} valid, {len(invalid_records)} invalid, {len(duplicate_records)} duplicates")
        return len(valid_records)
    except Exception as e:
        logging.error(f"Validation and staging failed: {str(e)}")
        raise AirflowException(f"Validation and staging failed: {str(e)}")
    
    
def load_to_final_table(**kwargs):
    """Load valid records from staging to final table"""
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # Verify table structure
        cursor.execute(f"DESCRIBE {FINAL_TABLE}")
        columns = [col[0] for col in cursor.fetchall()]
        
        required_columns = {
            'employee_id', 'firstname', 'lastname', 'salary',
            'email', 'billing_code', 'resource_id', 'created_at'
        }
        missing_columns = required_columns - set(columns)
        
        if missing_columns:
            raise AirflowException(
                f"Table {FINAL_TABLE} is missing required columns: {', '.join(missing_columns)}"
            )
        
        # Insert with duplicate handling on (employee_id, email) pair
        cursor.execute(
            f"""
            INSERT INTO {FINAL_TABLE} 
            (employee_id, firstname, lastname, salary, email, billing_code, resource_id, created_at)
            SELECT 
                employee_id, firstname, lastname, salary, email,
                billing_code, resource_id, created_at
            FROM {STAGING_TABLE}
            ON DUPLICATE KEY UPDATE
                firstname = VALUES(firstname),
                lastname = VALUES(lastname),
                salary = VALUES(salary),
                billing_code = VALUES(billing_code),
                resource_id = VALUES(resource_id),
                created_at = VALUES(created_at)
            """
        )
        
        affected_rows = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Loaded {affected_rows} records to final table")
        return affected_rows
    except Exception as e:
        logging.error(f"Final table load failed: {str(e)}")
        raise AirflowException(f"Final table load failed: {str(e)}")

def cleanup_staging(**kwargs):
    """Clean up staging table"""
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Staging table cleaned up")
        return True
    except Exception as e:
        logging.error(f"Cleanup failed: {str(e)}")
        raise AirflowException(f"Cleanup failed: {str(e)}")