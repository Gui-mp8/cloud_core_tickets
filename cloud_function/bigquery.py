from google.cloud import bigquery
import pandas as pd
import re

class BigQuery:
    def __init__(self):
        self.client = bigquery.Client()

    def clean_column_name(self, column_name):
        # Replace specified characters
        column_name = column_name.replace("ç", "c").replace("ã", "a")
        column_name = re.sub(r'[() \-:]', '_', column_name)
        return column_name.lower()

    def truncate_csv_file(self, storage_file_path: str, dataset_name: str, table_name: str):
        df = pd.read_csv(storage_file_path, sep=",", encoding="utf8", dtype=str)

        if df.empty:
            print("DataFrame is empty. Skipping.")
            return

        df.columns = [self.clean_column_name(col) for col in df.columns]

        schema = []
        for column, dtype in df.dtypes.items():
            if dtype == 'object':
                bq_type = 'STRING'

            schema.append(bigquery.SchemaField(column, bq_type))

        # Load DataFrame into BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        )

        table_ref = self.client.dataset(dataset_name).table(table_name)

        load_job = self.client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config,
        )

        load_job.result()

        print(f"Table: {table_ref.table_id} Created")

