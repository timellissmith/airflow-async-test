from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import pendulum
import yaml
import os
import asyncio
from typing import Any, Dict, Tuple

from triggers.mock_api import MockApiCallTrigger

YAML_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")


class DeferrableApiOperator(BaseOperator):
    """
    An operator that kicks off an API call and then defers itself,
    freeing up the worker slot entirely while waiting.
    """
    template_fields = ("call",)

    def __init__(self, call: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.call = call

    def execute(self, context):
        """
        The synchronous execution that happens on the worker.
        We immediately defer it to the Triggerer process.
        """
        call_id = self.call.get("id", "unknown_call")
        wait_time = float(self.call.get("wait", 1))
        
        self.log.info(f"Deferring task for API call {call_id}. Freeing up worker slot.")
        
        # Suspend task and yield to the Triggerer
        self.defer(
            trigger=MockApiCallTrigger(call_id=call_id, wait_time=wait_time),
            method_name="execute_complete",
            timeout=pendulum.duration(seconds=30),
        )

    def execute_complete(self, context, event=None):
        """
        This executes back on a worker node once the Triggerer yields a TriggerEvent.
        """
        if event and event.get("status") == "success":
            self.log.info(f"Successfully completed API call: {event['call_id']}")
            return event["call_id"]
        else:
            error_msg = event.get("error", "Unknown error") if event else "No event received."
            self.log.error(f"API call failed: {error_msg}")
            raise RuntimeError(f"API call failed: {error_msg}")


with DAG(
    dag_id="simulate_api_deferrable",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api_test", "deferrable", "async"],
) as dag:
    
    @task
    def load_api_calls():
        with open(YAML_FILE, "r") as f:
            calls = yaml.safe_load(f)
        # Limit to 10 for demonstration to avoid overflooding Airflow UI with 500 un-chunked deferrable tasks
        # In production this could be batched similarly to the chunked DAG
        return calls[:10]

    api_calls = load_api_calls()
    
    # Dynamically expand the deferrable operator. 
    # Each mapped instance will defer itself, meaning we can have 10,000 tasks
    # in the "deferred" state using exactly 0 active worker slots.
    run_deferred_calls = DeferrableApiOperator.partial(
        task_id="run_api_call"
    ).expand(call=api_calls)

    always_run_downstream = EmptyOperator(
        task_id="always_run_downstream",
        trigger_rule="all_done"
    )
    
    run_deferred_calls >> always_run_downstream
