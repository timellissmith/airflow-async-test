import os
import json
import asyncio
import aiohttp
from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import BaseOperator

# File path for lazy loading
NDJSON_FILE = os.path.join(os.path.dirname(__file__), "api_calls.ndjson")

class LazyAsyncApiOperator(BaseOperator):
    """
    An operator that lazy-loads API configurations from an NDJSON file
    and processes them asynchronously in a single task execution.
    """
    
    def __init__(self, ndjson_path: str, concurrency: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.ndjson_path = ndjson_path
        self.concurrency = concurrency

    async def call_mock_api(self, session, call):
        """Simulate an async API call."""
        call_id = call.get("id")
        url = call.get("url")
        wait_time = float(call.get("wait", 1))
        
        # We simulate the wait using asyncio.sleep
        await asyncio.sleep(wait_time)
        return {"id": call_id, "status": "success"}

    async def run_async_calls(self):
        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            results = []
            
            # Lazy load from NDJSON
            with open(self.ndjson_path, 'r') as f:
                for line in f:
                    if not line.strip():
                        continue
                    call = json.loads(line)
                    tasks.append(self.call_mock_api(session, call))
            
            self.log.info(f"Loaded {len(tasks)} API calls. Starting concurrent execution...")
            results = await asyncio.gather(*tasks)
            return results

    def execute(self, context):
        """The entry point for Airflow."""
        self.log.info(f"Reading from {self.ndjson_path}")
        results = asyncio.run(self.run_async_calls())
        self.log.info(f"Successfully processed {len(results)} API calls.")
        return len(results)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

with DAG(
    'simulate_api_lazy_async_operator',
    default_args=default_args,
    description='Benchmark 5000 API calls with single-task lazy async operator',
    schedule_interval=None,
    catchup=False,
    tags=['benchmark', 'lazy', 'async'],
) as dag:

    run_lazy_async = LazyAsyncApiOperator(
        task_id='run_5000_api_calls_lazy_async',
        ndjson_path=NDJSON_FILE,
        concurrency=500, # Higher concurrency for single task
    )
