import os
from databricks import sql
from configparser import ConfigParser

config = ConfigParser()
dirname = os.path.dirname(__file__)
config_filename = os.path.join(dirname, '../../config/my_config.cfg')
config.read(config_filename)

server_hostname = config.get('warehouse', 'server_hostname')
http_path = config.get('warehouse', 'http_path')
access_token = config.get('databricks_token', 'my_pat')

conn = sql.connect(server_hostname=server_hostname,
                  http_path=http_path,
                  access_token=access_token)


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