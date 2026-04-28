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

    def import_data_to_table(self, data, table_name):
        '''
        arguments: data (pandas DataFrame), table_name (str)
        
        This method imports data to a table in the database.
        '''
        try:
            print("Importing data to a table in the database...")
            data.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print(f"Data imported to table '{table_name}'.")
        except Exception as e:
            print("Error occurred while importing data to table in database")
            raise e

    def import_csv_to_table(self, file_path, table_name):
        '''
        arguments: file_path (str), table_name (str)
        
        Importing CSV Data to table in database
        '''
        try:
            print("Importing CSV Data to table in database...")
            df = pd.read_csv(file_path, encoding='latin1', sep=",")
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print(f"CSV data imported to table '{table_name}'.")
        except Exception as e:
            print("Error occurred while importing CSV Data to table in database")
            raise e

    def close_connection(self):
        print("Closing connection...")
        self.conn.close()
        self.engine.dispose()
