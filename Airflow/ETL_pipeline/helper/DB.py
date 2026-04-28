import pandas as pd
from sqlalchemy import create_engine
import urllib

class DatabaseManager:

    def __init__(self, host, user, password, database, port=3306):
        try:
            # Create connection string for SQLAlchemy engine
            # URL encode password to handle special characters
            password_encoded = urllib.parse.quote_plus(password)
            connection_string = f"mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{database}"
            self.engine = create_engine(connection_string)
            self.conn = self.engine.connect()
            print("Database connection established successfully.")
        except Exception as e:
            print("Error connecting to the database.")
            raise e

    def export_table_to_csv(self, table_name, csv_file_path):
        '''
        arguments: table_name (str), csv_file_path (str)

        This method exports a table from the database to a CSV file.
        '''
        try:
            print(f"Exporting table '{table_name}' to CSV file '{csv_file_path}'...")
            # Read the table into a DataFrame
            df = pd.read_sql_table(table_name, con=self.engine)
            # Save the DataFrame to a CSV file
            df.to_csv(csv_file_path, index=False)
            print(f"Table '{table_name}' exported to CSV file '{csv_file_path}'.")
        except Exception as e:
            print("Error occurred while exporting table to CSV file")
            raise e
        
    
    def close_connection(self):
        print("Closing connection...")
        self.conn.close()
        self.engine.dispose()
