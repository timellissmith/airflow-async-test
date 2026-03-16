from airflow import DAG
from airflow.decorators import task
import pendulum
import yaml
import os
import time

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")
MAX_CONCURRENT_TASKS = 50  # Set to None for no limit

with DAG(
    dag_id="simulate_api_dynamic",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "dynamic"],
) as dag:
    
    @task
    def load_api_calls():
        with open(YAML_FILE, "r") as f:
            calls = yaml.safe_load(f)
        return calls

    @task
    def execute_api_call(call):
        # Simulate blocking API call
        time.sleep(call.get("wait", 1))
        return f"Completed {call['id']}"

    # Load calls and dynamically map the API call execution
    calls_data = load_api_calls()
    
    if MAX_CONCURRENT_TASKS is not None:
        # override max_active_tis_per_dag before expanding
        results = execute_api_call.override(max_active_tis_per_dag=MAX_CONCURRENT_TASKS).expand(call=calls_data)
    else:
        # The .expand() operator creates one task instance per item in the list
        results = execute_api_call.expand(call=calls_data)
