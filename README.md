# Credit Risk Prediction Pipeline (GCP BigQuery + Cloud Composer)

## Overview

This project demonstrates how to build a scalable data pipeline on **Google Cloud Platform (GCP)** for credit risk prediction. The pipeline ingests, processes, and transforms data to generate meaningful insights using cloud-native services and workflow orchestration.

We are using:

- **BigQuery** for data storage, querying, and transformations
- **Cloud Composer (Apache Airflow on GCP)** for orchestration
- **Python** for data ingestion and generation
- **Google Cloud Storage (GCS)** for raw file storage
- **Looker Studio** for dashboards and visualization

Additionally, we are working on scalability by simulating increasing data volumes using scheduled data generation.

---

## Architecture Diagram

![Credit Risk Pipeline Architecture](A_digital_diagram_in_the_image_illustrates_a_Credi.png)

---

## Pipeline Structure

There are **two main DAG scripts** orchestrated in Cloud Composer:

### ✅ **1. Upload & Load DAG**
- **Purpose:** Uploads existing CSV data from the GCS bucket and loads it into the BigQuery raw table.
- **Flow:**
  1. Upload file from `gcs/data/credit_risk.csv`.
  2. Load the data into the `loan_applications` raw table in BigQuery.
  3. Apply transformations to create staging (`stg_credit_risk`) and curated (`loans_curated`) tables.
  4. Generate views for KPIs like `v_kpi_overall`, `v_default_by_intent`, and `v_default_by_risk_bucket`.
  5. Visualization is enabled in **Looker Studio** for dashboards and reports.

### ✅ **2. Data Generation DAG**
- **Purpose:** Automatically generates 30,000 random but realistic records every day to test pipeline scalability.
- **Flow:**
  1. Archive the existing dataset with timestamp-based naming.
  2. Create new records using Python with randomized data.
  3. Save the new file into the GCS bucket.
  4. Load the new data into the same raw table (`loan_applications`) to ensure growth over time.
  5. Downstream transformations and views remain consistent, allowing performance monitoring as the dataset grows.

---

## Dataset

Source1: [Kaggle Credit Risk Dataset](https://www.kaggle.com/)  
Stored in: **Google Cloud Storage bucket** and ingested into **BigQuery**.

Source2: Data ingestion is also done through pyhton script by generating 30k realistic random records everyday.
Stored in: **Google Cloud Storage bucket** and ingested into **BigQuery**.


---

## Tables & Views

- **Raw Table:** `loan_applications` — stores ingested data from CSV files and generated records.
- **Staging Table:** `loan_applications_stg` — cleansed and standardized version of raw data.
- **Curated Table:** `loan_applications_curated` — partitioned and clustered for optimized querying.
- **Views:**
  - `v_kpi_overall`: Key performance indicators for the entire dataset.
  - `v_default_by_intent`: Insights by loan intent.
  - `v_default_by_risk_bucket`: Insights by risk categories.

---

## Tools & Services Used

- **Cloud Composer / Apache Airflow** – Workflow orchestration
- **BigQuery** – Data warehousing, querying, and transformation
- **Google Cloud Storage (GCS)** – Raw data storage
- **Looker Studio** – Dashboard and visualization
- **Python** – Data generation and ingestion logic

---

## Future Enhancements

- Work on pipeline scalability and performance monitoring as the dataset grows.
- Explore advanced ML-driven credit risk modeling.
- Integrate additional data sources for deeper insights.

---

## GitHub Repository
[GitHub Repository Link](https://github.com/surya19970101/credit-risk-data-pipeline)


---

## How to Run

1. Upload the `credit_risk.csv` file to the `gcs/data/` folder in your bucket.
2. Trigger the **Upload & Load DAG** to ingest initial data.
3. Schedule the **Data Generation DAG** to run daily and simulate data growth.
4. Explore data via **BigQuery** and create dashboards in **Looker Studio**.

---