import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
key_file = '/home/sw3et1/Downloads/lustrous-bounty-489309-b0-06a20a44288a.json'
credentials = service_account.Credentials.from_service_account_file(key_file)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

query = """
SELECT *
FROM `bigquery-public-data.noaa_gsod.gsod2020`
LIMIT 10
"""

query_job = client.query(query)
query_result = query_job.result()
df = query_result.to_dataframe()
print(df)