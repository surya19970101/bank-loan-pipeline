CREATE OR REPLACE TABLE `credit_risk.loan_applications_curated`
PARTITION BY ingestion_date
CLUSTER BY default_flag, loan_intent AS
SELECT
  ingestion_date,
  loan_sk,
  person_age,
  person_income,
  person_emp_length,
  person_home_ownership,
  loan_amnt,
  loan_int_rate,
  loan_percent_income,
  loan_intent,
  loan_grade,
  cb_person_default_on_file,
  cb_person_cred_hist_length,
  default_flag,

  -- Derived features
  ROUND(loan_int_rate, 4)                                       AS int_rate_pct,
  ROUND(loan_percent_income, 4)                                 AS dti_ratio,
  CASE
    WHEN UPPER(loan_grade) IN ('A') THEN 'LOW'
    WHEN UPPER(loan_grade) IN ('B','C') THEN 'MEDIUM'
    WHEN UPPER(loan_grade) IN ('D','E','F','G') THEN 'HIGH'
    ELSE 'UNKNOWN'
  END AS risk_bucket
FROM `credit_risk.loan_applications_stg`;