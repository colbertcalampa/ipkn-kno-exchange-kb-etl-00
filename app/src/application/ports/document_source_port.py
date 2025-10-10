from abc import ABC, abstractmethod

class DocumentSourcePort(ABC):
    @abstractmethod
    def get_page(self, page_id: str) -> dict:
        pass