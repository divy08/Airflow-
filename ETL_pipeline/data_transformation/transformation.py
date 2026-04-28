import pandas as pd
import os
import zipfile

class DataTransformer:
    def __init__(self, input_folder: str, output_folder: str):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.aws_csv = None
        self.db_csv = None
        self.kaggle_csv = None

    def read_csvs(self):
        print("Reading individual CSV files...")
        self.aws_csv = pd.read_csv(os.path.join(self.input_folder, 'House_Price_AWS.csv'))
        self.db_csv = pd.read_csv(os.path.join(self.input_folder, 'House_Price_DB.csv'))
        self.kaggle_csv = pd.read_csv(os.path.join(self.input_folder, 'House_Price_Kaggle.csv'))
        print("CSV files read successfully.")

    def concatenate_all(self):
        print("Concatenating DataFrames...")
        if self.aws_csv is not None and self.db_csv is not None and self.kaggle_csv is not None:
            concatenated_df = pd.concat([self.aws_csv, self.db_csv, self.kaggle_csv], ignore_index=True)
            print("DataFrames concatenated successfully.")
            return concatenated_df
        else:
            print("Please read the CSV files first.")
            return None

    def save_output(self, df, filename='house_price_concatenated.csv'):
        os.makedirs(self.output_folder, exist_ok=True)
        output_path = os.path.join(self.output_folder, filename)
        df.to_csv(output_path, index=False)
        print(f"Saved concatenated dataset to: {output_path}")

    def clean_downloads_folder(self):
        print(f"Cleaning downloads folder: {self.input_folder}")
        for filename in os.listdir(self.input_folder):
            file_path = os.path.join(self.input_folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

    def zip_and_remove_file(self, file_path):
        """
        Compress the given file into a zip archive and delete the original file.
        
        Args:
            file_path (str): Full path to the file to be zipped and deleted.
        
        Returns:
            zip_file_path (str): Path to the created zip file.
        """
    
        # Create the ZIP file path (same name, .zip extension)
        zip_file_path = file_path.replace('.csv', '.zip')

        # Create ZIP file
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path, arcname=os.path.basename(file_path))

        print(f"File zipped successfully: {zip_file_path}")

        # Remove the original CSV
        os.remove(file_path)
        print(f"Original file deleted: {file_path}")

        return zip_file_path
    

    def run(self):
        self.read_csvs()
        concatenated_df = self.concatenate_all()
        if concatenated_df is not None:
            self.save_output(concatenated_df)
        


