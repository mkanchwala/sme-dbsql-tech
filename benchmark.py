import requests

from utils import get_config
from warehouses import _get_warehouse_id

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
warehouse_name = my_config.get('warehouse', 'name')
access_token = my_config.get('databricks_token', 'my_pat')

def get_queries_history():
    warehouse_id = _get_warehouse_id()

    response = requests.get(
        f"https://{server_hostname}/api/2.0/sql/history/queries/",
        headers={'Authorization': f'Bearer {access_token}'},
        json={
            "filter_by": {
                "statuses": [
                    "FINISHED"
                    ],
                "warehouse_ids": [
                    f"{warehouse_id}"
                    ]
            },
            "include_metrics": "true",
            "max_results": 100
        }
    )
    if response.status_code == 200:
        return response.json()
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))


if __name__ == '__main__':
  print(get_queries_history())