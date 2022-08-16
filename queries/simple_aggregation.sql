SELECT
  payment_type,
  SUM(total_amount) as total_paid
FROM
  sme_dbsql_perf.nyctaxi_yr
WHERE
  p_year = 2014
GROUP BY
  payment_type