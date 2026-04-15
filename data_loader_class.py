import os
import pandas as pd
from google.cloud import bigquery


class DataLoader:
    def __init__(self, key_file_path):
        if key_file_path:
            # Ustawienie zmiennej środowiskowej z kluczem do Google Cloud
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

        # Inicjalizacja klienta BigQuery
        self.client = bigquery.Client()
        self.data_frames = {}

    def make_a_query_and_save_to_class(self, query, name):
        query_job = self.client.query(query)
        self.data_frames[name] = query_job.to_dataframe()

    def save_all_df_to_csv(self, folder='data/query/'):
        os.makedirs(folder, exist_ok=True)
        for name, df in self.data_frames.items():
            df.to_csv(os.path.join(folder, f"{name}.csv"), index=False)

    def load_dfs_from_csv_to_class(self, folder='data/query/'):
        if not os.path.exists(folder):
            return 0

        files = [f for f in os.listdir(folder) if f.endswith('.csv')]
        for file in files:
            name = file.replace('.csv', '')
            self.data_frames[name] = pd.read_csv(os.path.join(folder, file), low_memory=False)

        return len(files)

    def get_df_from_class(self, name):
        return self.data_frames.get(name)

    def save_df_to_csv(self, df, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)

    def get_df_from_csv(self, path):
        return pd.read_csv(path, low_memory=False)