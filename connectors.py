import os
import pyodbc
from databricks import sql
import requests
from retrying import retry

from utils import get_config
from warehouses import get_http_path, create_warehouse, stop_warehouse, get_warehouse_id

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
access_token = my_config.get('databricks_token', 'my_pat')
driver_path = my_config.get('odbc', 'driver_path')
dirname = os.path.dirname(__file__)

@retry(wait_fixed=2000, stop_max_attempt_number=10)
def run_query_with_dbsql_cli(query_name="simple_aggregation.sql"):
  create_warehouse()
  http_path = get_http_path()

  sql_filename = os.path.join(dirname, 'queries', query_name)
  
  cmd = f"""dbsqlcli 
    --hostname '{server_hostname}' 
    --http-path '{http_path}' 
    --access-token '{access_token}' 
    -e {sql_filename}
    > /tmp/tmp.csv"""

  os.system(cmd.replace("\n", ""))
  os.remove("/tmp/tmp.csv")
  stop_warehouse()

@retry(wait_fixed=2000, stop_max_attempt_number=10)
def run_query_with_odbc(query_name="simple_aggregation.sql"):
  create_warehouse()
  http_path = get_http_path()

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
  cursor.close()
  conn.close()
  stop_warehouse()

@retry(wait_fixed=2000, stop_max_attempt_number=10)
def run_query_with_python_package(query_name="simple_aggregation.sql"):
  create_warehouse()
  http_path = get_http_path()

  conn = sql.connect(server_hostname=server_hostname,
                  http_path=http_path,
                  access_token=access_token)
  
  sql_filename = os.path.join(dirname, 'queries', query_name)
  fd = open(sql_filename, 'r')
  sql_query = fd.read()
  fd.close()
  
  cursor = conn.cursor()
  cursor.execute(sql_query)
  cursor.close()
  conn.close()
  stop_warehouse()

@retry(wait_fixed=2000, stop_max_attempt_number=10)
def run_query_with_api(query_name="simple_aggregation.sql"):
  create_warehouse()

  sql_filename = os.path.join(dirname, 'queries', query_name)
  fd = open(sql_filename, 'r')
  sql_query = fd.read()
  fd.close()

  endpoint = "api/2.0/sql/statements"
  url = f"https://{server_hostname}/{endpoint}"
  headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Host": server_hostname
  }
  body = {
    "statement": sql_query,
    "warehouse_id": str(get_warehouse_id())
  }
  r = requests.post(url, headers=headers, json=body)
  
  if r.status_code == 200:
      fetch_token = r.json()["execution_token"]
      state = r.json()["execution_status"]["state"]
      while state != "SUCCESS":
        r= requests.get(f"{url}/{fetch_token}", headers=headers)
        if r.status_code == 200:
          state = r.json()["execution_status"]["state"]
        else:
          print("Error: %s: %s" % (r.json()["error_code"], r.json()["message"]))
  else:
      print("Error: %s: %s" % (r.json()["error_code"], r.json()["message"]))
  stop_warehouse()

