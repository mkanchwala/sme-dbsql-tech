import requests
import time
import pandas as pd
from os import listdir
from os.path import isfile, join

from utils import get_config
from warehouses import get_warehouse_id
from connectors import run_query_with_dbsql_cli, run_query_with_odbc, run_query_with_python_package, run_query_with_api

my_config = get_config()
server_hostname = my_config.get('warehouse', 'server_hostname')
warehouse_name = my_config.get('warehouse', 'name')
access_token = my_config.get('databricks_token', 'my_pat')

metrics_of_interest = ["compilation_time_ms", "execution_time_ms", "task_total_time_ms", "result_fetch_time_ms"]

def _get_query_lookup_key():
    warehouse_id = get_warehouse_id()

    response = requests.get(
        f"https://{server_hostname}/api/2.0/sql/history/queries/",
        headers={'Authorization': f'Bearer {access_token}'},
        json={
            "filter_by": {
                "statuses": [
                    "FINISHED"
                    ],
                "warehouse_ids": [f"{warehouse_id}"]
            },
            "include_metrics": "true",
            "max_results": 1
        }
    )
    if response.status_code == 200:
        response = response.json()
        lookup_key = response["res"][0]["lookup_key"]
        return lookup_key
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))


def _get_query_detail(lookup_key):
    response = requests.get(
        f"https://{server_hostname}/api/2.0/sql/history/queries-by-lookup-keys?max_results=10&include_metrics=true&include_plans=false",
        headers={'Authorization': f'Bearer {access_token}'},
        json={
            "lookup_keys": [f"{lookup_key}"]
        }
    )
    if response.status_code == 200:
        response = response.json()
        query_metrics = response["res"][0]["queryInfo"]["metrics"]
        query_info = {k: query_metrics[k] if k in query_metrics.keys() else 0 for k in metrics_of_interest }
        return query_info
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))


def run_benchmark(nb_runs=3, python_package=True, odbc=True, dbsql_cli= True, api=True):
    df = pd.DataFrame(columns=["connector", "query"] + metrics_of_interest)

    queries_list = [f for f in listdir("queries") if isfile(join("queries", f)) and f[0] != '_'""]

    for query in queries_list:
        if python_package:
            for _ in range(nb_runs):
                run_query_with_python_package(query)
                lookup_key = _get_query_lookup_key()
                query_info = _get_query_detail(lookup_key)
                query_info["connector"] = "python_package"
                query_info["query"] = query
                df = df.append(query_info, ignore_index=True)
                print(df)
        
        if odbc:
            for _ in range(nb_runs):
                run_query_with_odbc(query)
                lookup_key = _get_query_lookup_key()
                query_info = _get_query_detail(lookup_key)
                query_info["connector"] = "odbc"
                query_info["query"] = query
                df = df.append(query_info, ignore_index=True)
                print(df)
        
        if dbsql_cli:
            for _ in range(nb_runs):
                run_query_with_dbsql_cli(query)
                lookup_key = _get_query_lookup_key()
                query_info = _get_query_detail(lookup_key)
                query_info["connector"] = "dbsql_cli"
                query_info["query"] = query
                df = df.append(query_info, ignore_index=True)
                print(df)

        if api:
            for _ in range(nb_runs):
                run_query_with_api(query)
                lookup_key = _get_query_lookup_key()
                query_info = _get_query_detail(lookup_key)
                query_info["connector"] = "api"
                query_info["query"] = query
                df = df.append(query_info, ignore_index=True)
                print(df)

    print(df)

if __name__ == '__main__':
  print(run_benchmark(nb_runs=1))
