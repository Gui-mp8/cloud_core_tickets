import os
import pandas as pd

from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './billing-accounts-dashboard-credentials.json'

df = pd.read_csv("gs://cloud_core/tickets-June-13-2024-15_22 - tickets-June-13-2024-15_22.csv.csv", sep=",", encoding="utf8")

client = bigquery.Client()



print(df)