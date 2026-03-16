from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import pendulum
import yaml
import os
import asyncio
import random

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")
CHUNK_SIZE = 50  # Number of API calls per mapped task
MAX_CONCURRENT_PER_CHUNK = 20  # Asyncio concurrency limit inside each task

class AsyncApiCallChunkOperator(BaseOperator):
    """
    Custom operator to execute a list of mock API calls asynchronously.
    Takes a direct list of `calls` (passed via .expand mapping).
    """
    # Define template fields so Airflow resolves the XCom mapping reference for `calls`
    template_fields = ("calls",)

    def __init__(self, calls=None, max_concurrent=50, **kwargs):
        super().__init__(**kwargs)
        self.calls = calls or []
        self.max_concurrent = max_concurrent

    async def _process_call_with_semaphore(self, semaphore, call):
        async with semaphore:
            max_retries = 3
            base_delay = 1
            for attempt in range(max_retries + 1):
                try:
                    await asyncio.sleep(call.get("wait", 1))
                    
                    if random.random() < 0.05:
                       raise Exception(f"Simulated network timeout for {call['id']}")
                       
                    return {"id": call['id'], "status": "success", "error": None}
                except Exception as e:
                    if attempt < max_retries:
                        delay = min((base_delay * (2 ** attempt)) + random.uniform(0, 1), 60)
                        self.log.warning(f"Error processing call {call['id']}: {str(e)}. Retrying in {delay:.2f}s...")
                        await asyncio.sleep(delay)
                    else:
                        self.log.error(f"Failed processing call {call['id']} after {max_retries} retries: {str(e)}")
                        return {"id": call['id'], "status": "failed", "error": str(e)}

    async def _run_all_async_calls(self, calls):
        semaphore = asyncio.Semaphore(self.max_concurrent)
        tasks = [self._process_call_with_semaphore(semaphore, call) for call in calls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results

    def execute(self, context):
        if not self.calls:
            self.log.info("No calls to process in this chunk.")
            return

        self.log.info(f"Processing chunk of {len(self.calls)} API calls asynchronously with max concurrency {self.max_concurrent}...")
        results = asyncio.run(self._run_all_async_calls(self.calls))
        
        success_count = sum(1 for r in results if isinstance(r, dict) and r["status"] == "success")
        error_count = len(results) - success_count
        
        self.log.info(f"Completed chunk calls. Successes: {success_count}, Errors: {error_count}")
        
        if error_count > 0:
            raise RuntimeError(f"Task failed because {error_count} out of {len(self.calls)} API calls failed in this chunk.")


with DAG(
    dag_id="simulate_api_async_chunked",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "async", "chunked"],
) as dag:
    
    @task
    def load_and_chunk_calls():
        with open(YAML_FILE, "r") as f:
            all_calls = yaml.safe_load(f)
            
        # Split the list of all API calls into chunks of size CHUNK_SIZE
        chunks = [all_calls[i:i + CHUNK_SIZE] for i in range(0, len(all_calls), CHUNK_SIZE)]
        print(f"Split {len(all_calls)} API calls into {len(chunks)} chunks.")
        # Returns a list of lists. E.g. [[call1, call2], [call3, call4], ...]
        return chunks

    chunks_list = load_and_chunk_calls()
    
    # Use Dynamic Task Mapping: Airflow will generate one AsyncApiCallChunkOperator task PER CHUNK.
    # If there are 500 calls and CHUNK_SIZE is 50, it creates 10 tasks.
    # Each of those 10 tasks receives a list of 50 calls via the `calls` argument and processes them asynchronously.
    run_chunks_async = AsyncApiCallChunkOperator.partial(
        task_id="run_async_chunk",
        max_concurrent=MAX_CONCURRENT_PER_CHUNK,
        retries=0
    ).expand(calls=chunks_list)
    
    always_run_downstream = EmptyOperator(
        task_id="always_run_downstream",
        trigger_rule="all_done"
    )
    
    run_chunks_async >> always_run_downstream
