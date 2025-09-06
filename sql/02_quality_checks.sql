SELECT
  (SELECT COUNT(*) FROM `credit_risk.loan_applications`) AS raw_rows,
  (SELECT COUNT(*) FROM `credit_risk.loan_applications`) AS stg_rows;

  SELECT
  SUM(CASE WHEN person_age IS NULL THEN 1 ELSE 0 END) AS null_age,
  SUM(CASE WHEN person_income IS NULL THEN 1 ELSE 0 END) AS null_income,
  SUM(CASE WHEN loan_amnt IS NULL THEN 1 ELSE 0 END) AS null_loan_amnt,
  SUM(CASE WHEN default_flag IS NULL THEN 1 ELSE 0 END) AS null_default_flag
FROM `credit_risk.loan_applications_stg`;

SELECT
  APPROX_QUANTILES(loan_amnt, 11) AS loan_amnt_deciles,
  APPROX_QUANTILES(loan_int_rate, 11) AS int_rate_deciles,
  APPROX_QUANTILES(person_income, 11) AS income_deciles
FROM `credit_risk.loan_applications`;