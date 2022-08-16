import os

from utils import get_config
from warehouses import create_warehouse_if_not_exists, stop_warehouse

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
access_token = my_config.get('databricks_token', 'my_pat')
dirname = os.path.dirname(__file__)

def run_query(query_name="simple_aggregation.sql"):
  http_path = create_warehouse_if_not_exists()
  
  stop_warehouse()

  sql_filename = os.path.join(dirname, 'queries', query_name)
  
  cmd = f"""dbsqlcli 
    --hostname '{server_hostname}' 
    --http-path '{http_path}' 
    --access-token '{access_token}' 
    -e {sql_filename}"""

  os.system(cmd.replace("\n", ""))

if __name__ == '__main__':
  run_query()