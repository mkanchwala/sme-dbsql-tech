import os
from databricks import sql

from utils import get_config
from warehouses import create_warehouse_if_not_exists, stop_warehouse

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
access_token = my_config.get('databricks_token', 'my_pat')
dirname = os.path.dirname(__file__)

def run_query(query_name="simple_aggregation.sql"):
  http_path = create_warehouse_if_not_exists()
  
  stop_warehouse()

  conn = sql.connect(server_hostname=server_hostname,
                  http_path=http_path,
                  access_token=access_token)
  
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