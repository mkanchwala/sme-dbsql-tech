import os
from databricks import sql

server_hostname = "e2-demo-field-eng.cloud.databricks.com"
http_path = "/sql/1.0/endpoints/bdc31958a55752d5"
access_token = os.environ['my_pat']

my_query = """
SELECT
  payment_type,
  SUM(total_amount) as total_paid
FROM
  sme_dbsql_perf.nyctaxi_yr
WHERE
  p_year = 2014
GROUP BY
  payment_type
"""

with sql.connect(server_hostname=server_hostname,
                 http_path=http_path,
                 access_token=access_token) as connection:
    with connection.cursor() as cursor:
        cursor.execute(my_query)
        result = cursor.fetchall()

        for row in result:
          print(row)