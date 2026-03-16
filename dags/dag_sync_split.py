from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import yaml
import os
import time

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")

def _execute_api_call(call, **kwargs):
    # Simulate blocking API call
    time.sleep(call.get("wait", 1))
    print(f"Completed {call['id']}")

# Open the config file at top level (DAG parsing time) to know how many tasks to create
with open(YAML_FILE, "r") as f:
    calls = yaml.safe_load(f)

with DAG(
    dag_id="simulate_api_sync_split",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "sync_split"],
) as dag:
    
    # Dynamically generate 500 distinct synchronous PythonOperator tasks
    for i, call in enumerate(calls):
        PythonOperator(
            task_id=f"run_call_{i}",
            python_callable=_execute_api_call,
            op_kwargs={"call": call},
        )
