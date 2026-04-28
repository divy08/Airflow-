import os
import sys
import boto3
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from botocore.exceptions import BotoCoreError, ClientError

class CustomException(Exception):
    def __init__(self, message, sys):
        super().__init__(message)

class ErrorEmailSender:
    def __init__(self, region_name, source_email, destination_email):
        self.session = boto3.Session(region_name=region_name)
        self.client = self.session.client("ses")
        self.source_email = source_email
        self.destination_email = destination_email

    def send_error_email(self, message_to_send, attachment=None, log_folder=None):
        if not isinstance(message_to_send, str):
            message_to_send = str(message_to_send)

        msg = MIMEMultipart()
        msg["Subject"] = "Airflow Notification - Error or Success"
        msg["From"] = self.source_email
        msg["To"] = self.destination_email

        body = MIMEText(message_to_send)
        msg.attach(body)

        # Attach DataFrame as CSV
        if attachment is not None:
            if isinstance(attachment, pd.DataFrame):
                if not attachment.empty:
                    csv_data = attachment.to_csv(index=False)
                    part = MIMEApplication(csv_data)
                    part.add_header("Content-Disposition", "attachment", filename="data.csv")
                    msg.attach(part)
            elif isinstance(attachment, str):
                if os.path.exists(attachment):
                    with open(attachment, "rb") as f:
                        attachment_data = f.read()
                    part = MIMEApplication(attachment_data)
                    part.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=os.path.basename(attachment),
                    )
                    msg.attach(part)
                else:
                    print(f"Attachment file {attachment} not found.")
            else:
                print("Invalid attachment type. Provide a DataFrame or file path.")

        # Attach all files in a folder (e.g., logs)
        if log_folder:
            for root, _, files in os.walk(log_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    with open(file_path, "rb") as f:
                        attachment_data = f.read()
                    part = MIMEApplication(attachment_data)
                    part.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=os.path.basename(file_path),
                    )
                    msg.attach(part)

        # Send email via SES
        try:
            response = self.client.send_raw_email(
                Source=self.source_email,
                Destinations=[self.destination_email],
                RawMessage={"Data": msg.as_string()},
            )
            print(f"Email sent successfully, message ID: {response['MessageId']}")

        except BotoCoreError as e:
            print(f"Error while sending email: {e}")
            raise CustomException(e, sys)

        except ClientError as e:
            print(f"Client error: {e.response['Error']['Message']}")
            raise CustomException(e, sys)

        except Exception as e:
            print(f"Unexpected error while sending email: {e}")
            raise CustomException(e, sys)
