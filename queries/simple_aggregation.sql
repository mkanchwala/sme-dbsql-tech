SELECT
  payment_type,
  SUM(total_amount) as total_paid
FROM
  alex.nyctaxi_yellowcab_dates
WHERE
  passenger_count > 1
GROUP BY
  payment_type