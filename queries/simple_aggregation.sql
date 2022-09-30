SELECT
  payment_type,
  SUM(total_amount) as total_paid
FROM
  sme_dbsql_perf.nyctaxi
WHERE
  passenger_count > 1
GROUP BY
  payment_type