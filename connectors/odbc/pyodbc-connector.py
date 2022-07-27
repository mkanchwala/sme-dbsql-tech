import os
import pyodbc
from configparser import ConfigParser

config = ConfigParser()
dirname = os.path.dirname(__file__)
config_filename = os.path.join(dirname, '../../config/my_config.cfg')
config.read(config_filename)

server_hostname = config.get('warehouse', 'server_hostname')
http_path = config.get('warehouse', 'http_path')
access_token = config.get('databricks_token', 'my_pat')
driver_path = config.get('odbc', 'driver_path')

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


def run_query(query_name="simple_aggregation.sql"):
  sql_filename = os.path.join(dirname, '../../queries', query_name)
  fd = open(sql_filename, 'r')
  sql_query = fd.read()
  fd.close()
  cursor = conn.cursor()
  cursor.execute(sql_query)
  for row in cursor.fetchall():
    print(row)


if __name__ == '__main__':
  run_query()