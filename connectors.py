import os
import pyodbc
from databricks import sql

from utils import get_config
from warehouses import create_warehouse_if_not_exists, stop_warehouse

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
access_token = my_config.get('databricks_token', 'my_pat')
driver_path = my_config.get('odbc', 'driver_path')
dirname = os.path.dirname(__file__)

def run_query_with_dbsql_cli(query_name="simple_aggregation.sql"):
  http_path = create_warehouse_if_not_exists()
  
  stop_warehouse()

  sql_filename = os.path.join(dirname, 'queries', query_name)
  
  cmd = f"""dbsqlcli 
    --hostname '{server_hostname}' 
    --http-path '{http_path}' 
    --access-token '{access_token}' 
    -e {sql_filename}
    > /tmp/tmp.csv"""

  os.system(cmd.replace("\n", ""))
  os.remove("/tmp/tmp.csv")

def run_query_with_odbc(query_name="simple_aggregation.sql"):
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

def run_query_with_python_package(query_name="simple_aggregation.sql"):
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
 