from app.src.domain.ports.recourse_trigger_port import RecourseTriggerPort
from typing import Any

class StepFunctionTriggerAdapter(RecourseTriggerPort):

    def trigger(self, page_id: str, event_type: str) -> Any:
        print(f"Step function activado para page {page_id} ")