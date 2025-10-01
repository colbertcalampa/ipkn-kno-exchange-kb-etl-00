from abc import ABC, abstractmethod
from typing import Any

class RecourseTriggerPort(ABC):
    @abstractmethod
    def trigger(self, page_id: str, event_type: str) -> Any:
        pass
