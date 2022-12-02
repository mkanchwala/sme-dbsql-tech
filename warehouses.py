import requests

from utils import get_config

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
warehouse_name = my_config.get('warehouse', 'name')
warehouse_size = my_config.get('warehouse', 'size')
access_token = my_config.get('databricks_token', 'my_pat')

def _list_warehouses():
    response = requests.get(
        f"https://{server_hostname}/api/2.0/sql/warehouses/",
        headers={'Authorization': f'Bearer {access_token}'}
    )
    if response.status_code == 200:
        return response.json()["warehouses"]
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))

def _warehouse_exists():
    warehouses = _list_warehouses()
    return warehouse_name in [w["name"] for w in warehouses]

def get_warehouse_id():
    warehouses = _list_warehouses()
    warehouse_id = [w["id"] for w in warehouses if w["name"] == warehouse_name][0]
    return warehouse_id

def get_http_path():
    warehouse_id = get_warehouse_id()
    response = requests.get(
        f"https://{server_hostname}/api/2.0/sql/warehouses/{warehouse_id}",
        headers={'Authorization': f'Bearer {access_token}'}
    )
    if response.status_code == 200:
        return response.json()["odbc_params"]["path"]
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))

def create_warehouse():
    if _warehouse_exists():
        print(f"Warehouse {warehouse_name} already exists")

    else:
        response = requests.post(
            f"https://{server_hostname}/api/2.0/sql/warehouses/",
            headers={'Authorization': f'Bearer {access_token}'},
            json={
                "name": f"{warehouse_name}",
                "cluster_size": f"{warehouse_size}",
                "min_num_clusters": 1,
                "max_num_clusters": 1,
                "auto_stop_mins": 10,
                "enable_serverless_compute": "true",
                "channel": {
                    "name": "CHANNEL_NAME_CURRENT"
                }
            }
        )
        if response.status_code == 200:
            print(f"Warehouse {warehouse_name} has been created")
        else:
            print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))


def stop_warehouse():
    warehouse_id = get_warehouse_id()
    response = requests.post(
        f"https://{server_hostname}/api/2.0/sql/warehouses/{warehouse_id}/stop",
        headers={'Authorization': f'Bearer {access_token}'},
    )
    if response.status_code == 200:
        print(f"Warehouse {warehouse_name} has been terminated")
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))