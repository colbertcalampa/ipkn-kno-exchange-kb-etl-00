from app.src.domain.ports.step_function_port import StepFunctionTriggerPort
from typing import Any

class StepFunctionTriggerAdapter(StepFunctionTriggerPort):

    def trigger(self, page_id: str, event_type: str) -> Any:
        print(f"Step function activado para page {page_id} ")