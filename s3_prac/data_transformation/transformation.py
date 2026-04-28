import pandas as pd
import os

class DataTransformer:
    def __init__(self, input_folder: str, output_folder: str):
        self.input_folder = input_folder
        self.output_folder = output_folder

    def read_csv_files(self):
        self.brands = pd.read_csv(os.path.join(self.input_folder, 'brands.csv'))
        self.categories = pd.read_csv(os.path.join(self.input_folder, 'categories.csv'))
        self.customers = pd.read_csv(os.path.join(self.input_folder, 'customers.csv'))
        self.order_items = pd.read_csv(os.path.join(self.input_folder, 'order_items.csv'))
        self.orders = pd.read_csv(os.path.join(self.input_folder, 'orders.csv'))
        self.products = pd.read_csv(os.path.join(self.input_folder, 'products.csv'))
        self.staffs = pd.read_csv(os.path.join(self.input_folder, 'staffs.csv'))
        self.stocks = pd.read_csv(os.path.join(self.input_folder, 'stocks.csv'))
        self.stores = pd.read_csv(os.path.join(self.input_folder, 'stores.csv'))

    def clean_data(self):
        dfs = [self.brands, self.categories, self.customers, self.order_items, self.orders,
               self.products, self.staffs, self.stocks, self.stores]

        for df in dfs:
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col].fillna('Unknown', inplace=True)
                else:
                    df[col].fillna(0, inplace=True)

    def correct_column_names(self):
        if 'Store ID' in self.stores.columns:
            self.stores.rename(columns={'Store ID': 'store_id'}, inplace=True)

    def transform_and_merge(self):
        products_full = self.products.merge(self.brands, on='brand_id', how='left') \
                                     .merge(self.categories, on='category_id', how='left')

        order_items_full = self.order_items.merge(products_full, on='product_id', how='left')
        orders_full = self.orders.merge(self.customers, on='customer_id', how='left')
        order_details = order_items_full.merge(orders_full, on='order_id', how='left')

        # order_details = order_details.merge(self.staffs, on='staff_id', how='left') \
        #                              .merge(self.stores, on='store_id', how='left')

        order_details = order_details.merge(self.stocks, on=['product_id', 'store_id'], how='left')

        return order_details

    def save_file(self, df, filename='merged_final.csv'):
        os.makedirs(self.output_folder, exist_ok=True)
        filepath = os.path.join(self.output_folder, filename)
        df.to_csv(filepath, index=False)
        print(f"Successfully saved merged file at: {filepath}")

    def clean_downloads_folder(self):
        print(f"Cleaning downloads folder: {self.input_folder}")
        for filename in os.listdir(self.input_folder):
            file_path = os.path.join(self.input_folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

    

    def run(self):
        self.read_csv_files()
        self.clean_data()
        self.correct_column_names()
        merged_df = self.transform_and_merge()
        self.save_file(merged_df)

