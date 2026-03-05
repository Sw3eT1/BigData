import pandas as pd
import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
KEY_FILE = os.environ.get('KEYFILEPATH')
credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def make_a_query(query):
    query_job = client.query(query)
    query_result = query_job.result()
    return query_result.to_dataframe()


df_gsod = make_a_query("""
SELECT *
FROM `bigquery-public-data.noaa_gsod.gsod2020`
LIMIT 10
""")

df_stations = make_a_query("""
SELECT *
FROM bigquery-public-data.noaa_gsod.stations
LIMIT 10
""")

df_stations = df_stations.rename(columns={"usaf": "stn"})

pd.concat([df_gsod, df_stations], axis=1).to_csv('merged_gsod.csv', index=False)