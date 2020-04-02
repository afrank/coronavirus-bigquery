#!/usr/bin/python3

from google.cloud import bigquery
from google.cloud import storage
import requests
from datetime import date

storage_client = storage.Client()
bucket_name = "afrank-coronavirus"
dataset = "coronavirus"
table = "covidtracking"

bucket = storage_client.bucket(bucket_name)

csv_blob = requests.get("http://covidtracking.com/api/states/daily.csv").text

upload_date = str(date.today())

object_name = f"covidtracking/states_daily_{upload_date}.csv"

blob = bucket.blob(object_name)

blob.upload_from_string(csv_blob)

print(f"CSV uploaded to {object_name}")

client = bigquery.Client()
table_ref = client.dataset(dataset).table(table)

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job_config.skip_leading_rows = 1
job_config.autodetect = True
job_config.source_format = bigquery.SourceFormat.CSV
uri = f"gs://{bucket_name}/{object_name}"
load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
print("Starting job {}".format(load_job.job_id))

load_job.result()
print("Job finished.")

destination_table = client.get_table(table_ref)
print("Loaded {} rows.".format(destination_table.num_rows))
