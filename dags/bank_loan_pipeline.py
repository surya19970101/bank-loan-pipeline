from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import os
import csv
import random
from google.cloud import bigquery, storage

default_args = {
    'owner': 'suraj',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_FILE_PATH = "/home/airflow/gcs/data/credit_risk_dataset.csv"
ARCHIVE_FOLDER = "/home/airflow/gcs/data/archive/"
GCS_BUCKET_NAME = "asia-south1-bank-loan-pipel-2d7d7a70-bucket/data"
PROJECT_ID = "bank-loan-analytics"
DATASET_ID = "credit_risk"
TABLE_ID = "loan_applications"

def create_csv(**context):
    # Initialize clients
    bq_client = bigquery.Client()
    storage_client = storage.Client()

    # Archive old file if it exists
    if os.path.exists(DATA_FILE_PATH):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # unique timestamp
        archive_name = f"credit_risk_dataset_{timestamp}.csv"
        archive_path = os.path.join(ARCHIVE_FOLDER, archive_name)
        os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
        os.rename(DATA_FILE_PATH, archive_path)
        print(f"Archived old file to {archive_path}")

    # Get max loan_id from BigQuery
    query = f"""
        SELECT MAX(CAST(SUBSTR(loan_id, 7) AS INT64)) AS max_id
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    max_id = 1000  # default if no records exist
    for row in result:
        if row.max_id is not None:
            max_id = row.max_id + 1

    print(f"Starting loan_id from {max_id}")

    # Create new CSV file
    with open(DATA_FILE_PATH, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow([
            "person_age",
            "person_income",
            "person_home_ownership",
            "person_emp_length",
            "loan_intent",
            "loan_grade",
            "loan_amnt",
            "loan_int_rate",
            "loan_status",
            "loan_percent_income",
            "cb_person_default_on_file",
            "cb_person_cred_hist_length",
            "loan_id",
            "created_date"
        ])
        
        # Generate 30,000 records
        for i in range(30000):
            person_age = random.randint(20, 60)
            person_income = random.randint(10000, 10000000)
            person_home_ownership = random.choice(["mortgage", "rent", "own", "other"])
            person_emp_length = round(random.uniform(0.0, 125.0), 1)
            loan_intent = random.choice(["DEBTCONSOLIDATION","HOMEIMPROVEMENT","PERSONAL","MEDICAL","EDUCATION","VENTURE"])
            loan_grade = random.choice(["A", "B", "C", "D", "E", "F", "G"])
            loan_amnt = random.randint(50000, 2000000)
            loan_int_rate = round(random.uniform(5.5, 13.0), 2)
            loan_status = random.randint(0, 1)
            loan_percent_income = round(random.uniform(0.02, 0.51), 2)
            cb_person_default_on_file = random.choice([True, False])
            cb_person_cred_hist_length = random.randint(0, 40)
            loan_id = f"CSTMR-{max_id + i}"
            created_date = datetime.now().isoformat()

            writer.writerow([
                person_age,
                person_income,
                person_home_ownership,
                person_emp_length,
                loan_intent,
                loan_grade,
                loan_amnt,
                loan_int_rate,
                loan_status,
                loan_percent_income,
                cb_person_default_on_file,
                cb_person_cred_hist_length,
                loan_id,
                created_date
            ])
    print(f"Created new CSV with 30,000 records at {DATA_FILE_PATH}")


with DAG(
    'bank_loan_pipeline',
    default_args=default_args,
    description='Pipeline for loan data processing',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    create_csv_task = PythonOperator(
        task_id='create_csv',
        python_callable=create_csv,
        provide_context=True
    )

    load_raw = BigQueryInsertJobOperator(
        task_id='load_raw_data',
        configuration={
            "load": {
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_ID
                },
                #"sourceUris": [f"gs:/{DATA_FILE_PATH}"],
                "sourceUris": [f"gs://{GCS_BUCKET_NAME}/credit_risk_dataset.csv"],
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "fieldDelimiter": ",",
                "skipLeadingRows": 1,
                "schema": {
                    "fields": [
                        {"name": "person_age", "type": "INTEGER"},
                        {"name": "person_income", "type": "INTEGER"},
                        {"name": "person_home_ownership", "type": "STRING"},
                        {"name": "person_emp_length", "type": "FLOAT"},
                        {"name": "loan_intent", "type": "STRING"},
                        {"name": "loan_grade", "type": "STRING"},
                        {"name": "loan_amnt", "type": "INTEGER"},
                        {"name": "loan_int_rate", "type": "FLOAT"},
                        {"name": "loan_status", "type": "INTEGER"},
                        {"name": "loan_percent_income", "type": "FLOAT"},
                        {"name": "cb_person_default_on_file", "type": "BOOLEAN"},
                        {"name": "cb_person_cred_hist_length", "type": "INTEGER"},
                        {"name": "loan_id", "type": "STRING"},
                        {"name": "created_date", "type": "TIMESTAMP"}
                    ]
                }
            }
        }
    )

    run_staging = BigQueryInsertJobOperator(
        task_id='run_staging',
        configuration={
            "query": {
                "query": open('/home/airflow/gcs/dags/01_staging.sql').read(),
                "useLegacySql": False,
            }
        }
    )

    run_curated = BigQueryInsertJobOperator(
        task_id='run_curated',
        configuration={
            "query": {
                "query": open('/home/airflow/gcs/dags/03_curated.sql').read(),
                "useLegacySql": False,
            }
        }
    )

    run_kpis = BigQueryInsertJobOperator(
        task_id='run_kpis',
        configuration={
            "query": {
                "query": open('/home/airflow/gcs/dags/04_views_kpis.sql').read(),
                "useLegacySql": False,
            }
        }
    )

    create_csv_task >> load_raw >> run_staging >> run_curated >> run_kpis
