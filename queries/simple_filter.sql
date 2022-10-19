SELECT 
  * 
FROM 
  sme_dbsql_perf.nyctaxi
WHERE
  passenger_count > 50
LIMIT 1000000
