from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'suraj',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bank_loan_pipeline',
    default_args=default_args,
    description='Pipeline for loan data processing',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Upload file to GCS using BashOperator
    upload_to_gcs = BashOperator(
        task_id='upload_to_gcs',
        bash_command="""
        gsutil cp /home/airflow/gcs/data/credit_risk_dataset.csv gs://asia-south1-bank-loan-pipel-2d7d7a70-bucket/credit_risk.csv
        """
    )

    # Load raw data into BigQuery
    # load_raw = BigQueryInsertJobOperator(
    #     task_id='load_raw_data',
    #     configuration={
    #         "query": {
    #             "query": """
    #             INSERT INTO `bank-loan-analytics.credit_risk.loan_applications` 
    #             SELECT * FROM `bank-loan-analytics.credit_risk.loan_applications_stg`
    #             """,
    #             "useLegacySql": False,
    #         }
    #     }
    # )

    load_raw = BigQueryInsertJobOperator(
    task_id='load_raw_data',
    configuration={
        "load": {
            "destinationTable": {
                "projectId": "bank-loan-analytics",
                "datasetId": "credit_risk",
                "tableId": "loan_applications"
            },
            "sourceUris": ["gs://asia-south1-bank-loan-pipel-2d7d7a70-bucket/credit_risk.csv"],
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
                    {"name": "cb_person_cred_hist_length", "type": "INTEGER"}
                ]
            }
        }
    }
)


    # Run staging transformation
    run_staging = BigQueryInsertJobOperator(
        task_id='run_staging',
        configuration={
            "query": {
                "query": open('/home/airflow/gcs/dags/01_staging.sql').read(),
                "useLegacySql": False,
            }
        }
    )

    # Run curated transformation
    run_curated = BigQueryInsertJobOperator(
        task_id='run_curated',
        configuration={
            "query": {
                "query": open('/home/airflow/gcs/dags/03_curated.sql').read(),
                "useLegacySql": False,
            }
        }
    )

    # Run KPI views
    run_kpis = BigQueryInsertJobOperator(
        task_id='run_kpis',
        configuration={
            "query": {
                "query": open('/home/airflow/gcs/dags/04_views_kpis.sql').read(),
                "useLegacySql": False,
            }
        }
    )

    # Define task dependencies
    upload_to_gcs >> load_raw >> run_staging >> run_curated >> run_kpis