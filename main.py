import os
# import pandas as pd
# from google.cloud import bigquery
from bigquery import BigQuery

# Set the environment variable for authentication
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './billing-accounts-dashboard-credentials.json'

# Load the CSV file into a DataFrame
# df = pd.read_csv("gs://cloud_core/tickets-June-13-2024-15_22 - tickets-June-13-2024-15_22.csv.csv", sep=",", encoding="utf8")
def main(requests):
    BigQuery().truncate_csv_file(
        storage_file_path="gs://cloud_core/tickets-June-13-2024-15_22 - tickets-June-13-2024-15_22.csv.csv",
        dataset_name="cloud_core",
        table_name="tickets"
    )
