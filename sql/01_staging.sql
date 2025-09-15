CREATE OR REPLACE TABLE `credit_risk.loan_applications_stg` AS
WITH base AS (
  SELECT
    FARM_FINGERPRINT(
      CONCAT(
        COALESCE(CAST(person_age AS STRING), ''),
        '|', COALESCE(CAST(person_income AS STRING), ''),
        '|', COALESCE(CAST(loan_amnt AS STRING), ''),
        '|', COALESCE(loan_intent, ''),
        '|', COALESCE(loan_grade, ''),
        '|', COALESCE(CAST(cb_person_default_on_file AS STRING), '')
      )
    ) AS loan_sk,

    SAFE_CAST(person_age AS INT64)                         AS person_age,
    SAFE_CAST(person_income AS INT64)                      AS person_income,
    SAFE_CAST(person_emp_length AS INT64)                  AS person_emp_length,
    SAFE_CAST(loan_amnt AS INT64)                          AS loan_amnt,
    SAFE_CAST(loan_int_rate AS FLOAT64)                    AS loan_int_rate,
    SAFE_CAST(loan_percent_income AS FLOAT64)              AS loan_percent_income,
    SAFE_CAST(cb_person_cred_hist_length AS INT64)         AS cb_person_cred_hist_length,

    UPPER(TRIM(loan_intent))                               AS loan_intent,
    UPPER(TRIM(loan_grade))                                AS loan_grade,
    UPPER(TRIM(person_home_ownership))                     AS person_home_ownership,
    UPPER(TRIM(CAST(cb_person_default_on_file AS STRING))) AS cb_person_default_on_file,

    CASE
      WHEN LOWER(CAST(loan_status AS STRING)) IN ('1','y','yes','true','default','defaulter') THEN TRUE
      WHEN LOWER(CAST(loan_status AS STRING)) IN ('0','n','no','false','paid','good') THEN FALSE
      ELSE NULL
    END AS default_flag,

    CURRENT_DATE() AS ingestion_date
  FROM `credit_risk.loan_applications`
)
-- basic sanity filters to remove impossible values
SELECT
  loan_sk, person_age, person_income, person_emp_length, loan_amnt, loan_int_rate,
  loan_percent_income, cb_person_cred_hist_length, loan_intent, loan_grade,
  person_home_ownership, cb_person_default_on_file, default_flag, ingestion_date
FROM base
WHERE
  person_age BETWEEN 18 AND 100
  AND person_income IS NOT NULL AND person_income > 0
  AND loan_amnt IS NOT NULL AND loan_amnt > 0;