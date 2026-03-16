from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import yaml
import os
import asyncio

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")

async def _process_call_with_semaphore(semaphore, call):
    async with semaphore:
        # Simulate an asynchronous API call
        await asyncio.sleep(call.get("wait", 1))
        return f"Completed {call['id']}"
    
async def _run_all_async_calls(calls, max_concurrent=50):
    semaphore = asyncio.Semaphore(max_concurrent)
    # Create all tasks bound by the semaphore
    tasks = [_process_call_with_semaphore(semaphore, call) for call in calls]
    # Run all tasks concurrently and wait for them to finish
    await asyncio.gather(*tasks)

def process_async_calls(**kwargs):
    with open(YAML_FILE, "r") as f:
        calls = yaml.safe_load(f)
    
    print(f"Processing {len(calls)} API calls asynchronously...")
    # Execute the asyncio event loop
    asyncio.run(_run_all_async_calls(calls))
    print("Completed all calls concurrently.")

with DAG(
    dag_id="simulate_api_async",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "async"],
) as dag:
    
    run_async = PythonOperator(
        task_id="run_async",
        python_callable=process_async_calls,
    )
