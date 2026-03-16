from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio
from typing import Any, Dict, Tuple

class MockApiCallTrigger(BaseTrigger):
    """
    An Airflow Trigger that suspends execution in the database and waits
    asynchronously without consuming a worker slot.
    """
    def __init__(self, call_id: str, wait_time: float):
        super().__init__()
        self.call_id = call_id
        self.wait_time = wait_time

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        # This tells the Triggerer process where to find this class
        # It must be relative to the $DAGS_FOLDER (e.g., 'triggers.mock_api.MockApiCallTrigger')
        return (
            "triggers.mock_api.MockApiCallTrigger",
            {"call_id": self.call_id, "wait_time": self.wait_time},
        )

    async def run(self):
        """
        This runs in the Airflow Triggerer process (which runs an asyncio event loop).
        """
        try:
            # Simulate the asynchronous network request waiting
            await asyncio.sleep(self.wait_time)
            
            # Send an event back to the scheduler to resume the worker task
            yield TriggerEvent({"status": "success", "call_id": self.call_id})
        except Exception as e:
            yield TriggerEvent({"status": "failed", "call_id": self.call_id, "error": str(e)})
