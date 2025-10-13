from abc import ABC, abstractmethod


class LandingZonePort(ABC):
    @abstractmethod
    def save(self, key: str, content: dict) -> dict:
        pass

    @abstractmethod
    def get_document_from_uri(self, document_uri: str) -> dict:
        pass
