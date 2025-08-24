import pandas as pd
from google.cloud import storage, bigquery

# Update these with your details
PROJECT_ID = "bank-loan-analytics"
BUCKET_NAME = "credit-risk-data-surya"
DATASET_ID = "credit_risk"
TABLE_ID = "loan_applications"
CSV_FILE = "C:/Users/surya/bank-loan-pipeline/data/raw/credit_risk_dataset.csv"  # Path to your dataset

def upload_to_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob("credit_risk_dataset.csv")
    blob.upload_from_filename(CSV_FILE)

    print(f"✅ File uploaded to GCS: gs://{BUCKET_NAME}/credit_risk_dataset.csv")

def load_to_bigquery():
    bq_client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{BUCKET_NAME}/credit_risk_dataset.csv"
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Waits for job to complete

    print(f"✅ Data loaded into BigQuery table: {table_ref}")

if __name__ == "__main__":
    upload_to_gcs()
    load_to_bigquery() 
