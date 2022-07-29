import os
import pyodbc

from utils import get_config
from warehouses import create_warehouse_if_not_exists, stop_warehouse

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
access_token = my_config.get('databricks_token', 'my_pat')
driver_path = my_config.get('odbc', 'driver_path')
dirname = os.path.dirname(__file__)

def run_query(query_name="simple_aggregation.sql"):
  http_path = create_warehouse_if_not_exists()
  
  stop_warehouse()

  conn = pyodbc.connect(f"Driver={driver_path};" +
                      f"HOST={server_hostname};" +
                      "PORT=443;" +
                      "Schema=default;" +
                      "SparkServerType=3;" +
                      "AuthMech=3;" +
                      "UID=token;" +
                      f"PWD={access_token};" +
                      "ThriftTransport=2;" +
                      "SSL=1;" +
                      f"HTTPPath={http_path}",
                      autocommit=True)
  
  sql_filename = os.path.join(dirname, 'queries', query_name)
  fd = open(sql_filename, 'r')
  sql_query = fd.read()
  fd.close()
  
  cursor = conn.cursor()
  cursor.execute(sql_query)
 
  for row in cursor.fetchall():
    print(row)

if __name__ == '__main__':
  run_query()
