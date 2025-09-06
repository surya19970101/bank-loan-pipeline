CREATE OR REPLACE VIEW `credit_risk.loan_applications_kpi_overall_v` AS
SELECT
  COUNT(*) AS total_loans,
  SUM(CASE WHEN default_flag THEN 1 ELSE 0 END) AS total_defaults,
  SAFE_DIVIDE(SUM(CASE WHEN default_flag THEN 1 ELSE 0 END), COUNT(*)) AS default_rate,
  AVG(loan_int_rate) AS avg_interest_rate,
  AVG(loan_percent_income) AS avg_dti_ratio,
  AVG(person_income) AS avg_income
FROM `credit_risk.loan_applications_curated`;

--select * from `credit_risk.loan_applications_kpi_overall_v`;

CREATE OR REPLACE VIEW `credit_risk.loan_applications_default_by_intent_v` AS
SELECT
  loan_intent,
  COUNT(*) AS loans,
  SUM(CASE WHEN default_flag THEN 1 ELSE 0 END) AS defaults,
  SAFE_DIVIDE(SUM(CASE WHEN default_flag THEN 1 ELSE 0 END), COUNT(*)) AS default_rate
FROM `credit_risk.loan_applications_curated`
GROUP BY loan_intent
ORDER BY default_rate DESC;

--SELECT * FROM `credit_risk.loan_applications_default_by_intent_v`;

CREATE OR REPLACE VIEW `credit_risk.loan_applications_default_by_risk_bucket_v` AS
SELECT
  risk_bucket,
  COUNT(*) AS loans,
  SUM(CASE WHEN default_flag THEN 1 ELSE 0 END) AS defaults,
  SAFE_DIVIDE(SUM(CASE WHEN default_flag THEN 1 ELSE 0 END), COUNT(*)) AS default_rate
FROM `credit_risk.loan_applications_curated`
GROUP BY risk_bucket
ORDER BY default_rate DESC;

--SELECT * FROM `credit_risk.loan_applications_default_by_risk_bucket_v`;
