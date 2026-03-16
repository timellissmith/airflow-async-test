from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import yaml
import os
import asyncio
import random

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")

class AsyncApiCallOperator(BaseOperator):
    """
    Custom operator to execute a list of mock API calls asynchronously.
    It catches exceptions gracefully, ensures all calls attempt to execute,
    and fails the task at the end if any errors occurred.
    """
    def __init__(self, yaml_filepath, max_concurrent=50, **kwargs):
        super().__init__(**kwargs)
        self.yaml_filepath = yaml_filepath
        self.max_concurrent = max_concurrent

    async def _process_call_with_semaphore(self, semaphore, call):
        async with semaphore:
            try:
                # Simulate an asynchronous API call
                await asyncio.sleep(call.get("wait", 1))
                
                # Simulate a random error for demonstration purposes (~5% chance)
                if random.random() < 0.05:
                   raise Exception(f"Simulated network timeout for {call['id']}")
                   
                return {"id": call['id'], "status": "success", "error": None}
            except Exception as e:
                self.log.error(f"Error processing call {call['id']}: {str(e)}")
                return {"id": call['id'], "status": "failed", "error": str(e)}

    async def _run_all_async_calls(self, calls):
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Create all tasks bound by the semaphore
        tasks = [self._process_call_with_semaphore(semaphore, call) for call in calls]
        
        # Run all tasks concurrently and wait for all of them to finish
        # return_exceptions=True ensures that an exception in one task
        # doesn't cancel the others.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results

    def execute(self, context):
        with open(self.yaml_filepath, "r") as f:
            calls = yaml.safe_load(f)
        
        self.log.info(f"Processing {len(calls)} API calls asynchronously with max concurrency {self.max_concurrent}...")
        
        # Execute the asyncio event loop
        results = asyncio.run(self._run_all_async_calls(calls))
        
        # Analyze results
        success_count = sum(1 for r in results if isinstance(r, dict) and r["status"] == "success")
        error_count = len(results) - success_count
        
        self.log.info(f"Completed all calls. Successes: {success_count}, Errors: {error_count}")
        
        # If there were any errors, raise an exception to fail the task
        if error_count > 0:
            raise RuntimeError(f"Task failed because {error_count} out of {len(calls)} API calls failed.")


with DAG(
    dag_id="simulate_api_async_custom_operator",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "async", "custom_operator"],
) as dag:
    
    run_async_custom = AsyncApiCallOperator(
        task_id="run_async_calls_with_error_handling",
        yaml_filepath=YAML_FILE,
        max_concurrent=50
    )
    
    # Dummy task that runs regardless of whether the upstream task fails or succeeds
    always_run_downstream = EmptyOperator(
        task_id="always_run_downstream",
        trigger_rule="all_done"
    )
    
    run_async_custom >> always_run_downstream
