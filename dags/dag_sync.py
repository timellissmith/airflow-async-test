from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import yaml
import os
import time

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")

def process_sync_calls(**kwargs):
    with open(YAML_FILE, "r") as f:
        calls = yaml.safe_load(f)
    
    print(f"Processing {len(calls)} API calls synchronously...")
    for call in calls:
        # Simulate blocking API call
        time.sleep(call.get("wait", 1))
    print("Completed all calls.")

with DAG(
    dag_id="simulate_api_sync",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "sync"],
) as dag:
    
    run_sync = PythonOperator(
        task_id="run_sync",
        python_callable=process_sync_calls,
    )
