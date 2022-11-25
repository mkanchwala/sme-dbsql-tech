from time import sleep
import requests
import pandas as pd
from os import listdir
from os.path import isfile

from utils import get_config
from warehouses import get_warehouse_id
import connectors
import os

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
        query_status = response["res"][0]["queryInfo"]["status"]
        query_metrics = response["res"][0]["queryInfo"]["metrics"]
        query_info = {k: query_metrics[k] if k in query_metrics.keys() else 0 for k in metrics_of_interest }
        return query_status, query_info
    else:
        print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))


def run_benchmark(nb_runs=5):
    df = pd.DataFrame(columns=["connector", "query", "status"] + metrics_of_interest)

    queries_list = [f for f in listdir("queries") if isfile(f"queries/{f}") and f[0] != '_'""]

    for query in queries_list:
        for benchmark_name in ['python_package','odbc','dbsql_cli','api','sqlalchemy']:
            if my_config.get('benchmark', benchmark_name)!='False':
                print(f'Running benchmark {benchmark_name}')
                for _ in range(nb_runs):
                    sleep(10) 
                    getattr(connectors, f"run_query_with_{benchmark_name}")(query)
                    lookup_key = _get_query_lookup_key()
                    query_status, query_info = _get_query_detail(lookup_key)
                    query_info["status"] = query_status
                    query_info["connector"] = benchmark_name
                    query_info["query"] = query
                    df = df.append(query_info, ignore_index=True)
                    print(df)

    print(df)
    output_folder = my_config.get('benchmark', 'output')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    df.to_csv(f"{output_folder}/benchmark_output.csv")

if __name__ == '__main__':
  run_benchmark(int(my_config.get('benchmark', 'nb_runs')))
