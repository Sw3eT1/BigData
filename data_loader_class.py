import pandas as pd

from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

class DataLoader:
    def __init__(self, key_file_path):
        pass
        self.key_file_path = key_file_path
        self.credentials = service_account.Credentials.from_service_account_file(self.key_file_path)
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.loaded_data = {}


    def make_a_query_and_save_to_class(self, query, name):
        query_job = self.client.query(query)
        df = query_job.result().to_dataframe()
        self.loaded_data[name] = df
        return True

    def get_df_from_class(self, key):
        return self.loaded_data[key]

    def load_dfs_from_csv_to_class(self, folder="data"):

        folder_path = Path(folder)
        folder_path.mkdir(exist_ok=True)

        csv_files = folder_path.glob("*.csv")

        count = 0
        for file_path in csv_files:
            df = pd.read_csv(file_path)
            self.loaded_data[file_path.stem] = df
            count += 1
        return count


    @staticmethod
    def get_df_from_csv(file_name):
        df = pd.read_csv(file_name)
        return df

    @staticmethod
    def save_df_to_csv(df, file_name):
        df.to_csv(file_name, index=False)
        return True

    def save_all_df_to_csv(self, folder="data"):
        folder_path = Path(folder)
        folder_path.mkdir(exist_ok=True)

        for name, df in self.loaded_data.items():
            df.to_csv(folder_path / f"{name}.csv", index=False)
        return True
