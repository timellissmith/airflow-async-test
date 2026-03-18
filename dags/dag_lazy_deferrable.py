import os
import json
from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
# Using import string to be safer
from composer_custom_triggers.triggers.mock_api import MockApiCallTrigger

# File path for lazy loading
NDJSON_FILE = os.path.join(os.path.dirname(__file__), "api_calls.ndjson")

class DeferrableApiOperator(BaseOperator):
    template_fields = ("api_call_config",)
    def __init__(self, api_call_config: dict, **kwargs):
        super().__init__(**kwargs)
        self.api_call_config = api_call_config
    def execute(self, context):
        call_id = self.api_call_config.get("id", "unknown_call")
        wait_time = float(self.api_call_config.get("wait", 1))
        self.defer(
            trigger=MockApiCallTrigger(call_id=call_id, wait_time=wait_time),
            method_name="execute_complete",
        )
    def execute_complete(self, context, event=None):
        return event["call_id"]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    'simulate_api_lazy_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def load_api_calls(file_path):
    calls = []
    with open(file_path, "r") as f:
        for line in f:
            if line.strip():
                calls.append(json.loads(line))
    return calls

loader = PythonOperator(
    task_id='load_ndjson',
    python_callable=load_api_calls,
    op_kwargs={'file_path': NDJSON_FILE},
    dag=dag,
)

run_api_calls = DeferrableApiOperator.partial(
    task_id='run_deferrable_api_call',
    dag=dag,
).expand(
    api_call_config=loader.output
)
