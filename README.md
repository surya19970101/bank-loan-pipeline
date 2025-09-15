# Credit Risk Prediction Pipeline (GCP BigQuery + Cloud Composer)

## 📖 Overview
This project demonstrates how to build a **scalable, automated data pipeline** on **Google Cloud Platform (GCP)** for credit risk analysis.  
It covers data ingestion, transformation, orchestration, and visualization using cloud-native tools.

### Tools & Technologies Used:
- **Google Cloud Storage (GCS)** – for storing raw data files
- **BigQuery** – for data warehousing and SQL-based transformations
- **Cloud Composer (Apache Airflow)** – for orchestrating and scheduling workflows
- **Python** – for data preprocessing and pipeline automation
- **Looker Studio** – for creating dashboards and visual insights

---

## 📂 Dataset
- **Source**: [Kaggle Credit Risk Dataset](https://www.kaggle.com/)  
- **Purpose**: Analyze loan applications and defaults to predict credit risk
- **Processing**: Uploaded to a GCS bucket and ingested into BigQuery for further transformations

---

## 🗂 Folder Structure

bank-loan-pipeline/
├── dags/
│ └── bank_loan_pipeline.py # Airflow DAG definition
├── data/
│ └── credit_risk.csv # Raw dataset (optional or reference)
├── sql/
│ ├── 01_staging.sql # Data cleaning and standardization
│ ├── 03_curated.sql # Final loan applications table
│ └── 04_views_kpis.sql # KPI and analytical views
└── README.md # Project documentation


---

## ✅ Project Workflow & Milestones

### **Day 1 – Setup**
- Selected the Credit Risk dataset from Kaggle
- Defined the folder structure and configured the environment
- Created initial Python scripts to handle data upload and ingestion

### **Day 2 – Cloud Infrastructure**
- Created GCS bucket to store raw credit risk data
- Created BigQuery dataset and tables for structured data storage
- Uploaded data from GCS into BigQuery using Airflow DAGs
- Implemented staging and curated data layers

### **Day 3 – Transformations & Insights**
- Created `stg_credit_risk` to clean, standardize, and format raw data
- Designed `loans_curated` table partitioned by ingestion date and clustered for performance
- Built analytical views to measure KPIs:
  - `v_kpi_overall`: default rates and loan distributions
  - `v_default_by_intent`: loan defaults by loan purpose
  - `v_default_by_risk_bucket`: risk analysis across different customer segments

### Next Steps
- Integrate Looker Studio for visualization
- Extend the pipeline for automated retraining and predictions

---

## 🚀 How to Run This Project
1. Upload the raw dataset (`credit_risk.csv`) to the `/data/` folder in your Composer environment.
2. Verify that the GCS bucket and BigQuery dataset are configured.
3. Trigger the `bank_loan_pipeline` DAG from the Airflow UI.
4. Monitor the logs to ensure each task completes successfully.
5. Explore transformed tables and views in BigQuery.

---

## 📈 Key Learnings
- How to build end-to-end data pipelines using cloud services
- Best practices for data partitioning and clustering in BigQuery
- Orchestrating workflows with Cloud Composer
- Handling permissions, debugging, and monitoring cloud-based pipelines

---

## 📂 Repository
[GitHub Repository Link](https://github.com/surya19970101/bank-loan-pipeline)

---
