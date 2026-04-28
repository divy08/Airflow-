from googleapiclient.discovery import build
from google.oauth2 import service_account
import io
from googleapiclient.http import MediaIoBaseDownload
import os

# Setup credentials and Google Drive service
creds = service_account.Credentials.from_service_account_file(
    'C:/Users/DivyVishwakarma/Desktop/Airflow/gcp_s3_etl/dags/gcp-s3-etl-f0f35ccc96f0.json',
    scopes=['https://www.googleapis.com/auth/drive.readonly']
)

service = build('drive', 'v3', credentials=creds)

# Folder ID provided directly
folder_id = '1yd3gaLu46SKrdZMWERWeNIsUua0yJQJ3'

# Query to list all files in the specified folder
query = f"'{folder_id}' in parents and trashed=false"
results = service.files().list(q=query, fields="files(id, name)").execute()
files = results.get('files', [])

if not files:
    print("No files found in the folder.")
else:
    print(f"Found {len(files)} file(s) in folder. Starting download...")

    # Create a local directory to store downloaded files
    os.makedirs('downloads', exist_ok=True)

    for file in files:
        file_id = file['id']
        file_name = file['name']
        request = service.files().get_media(fileId=file_id)
        file_path = os.path.join('downloads', file_name)

        with open(file_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Downloading {file_name}: {int(status.progress() * 100)}%")

    print("All files downloaded successfully.")
