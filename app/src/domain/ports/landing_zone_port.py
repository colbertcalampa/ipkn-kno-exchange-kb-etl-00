from abc import ABC, abstractmethod


class LandingZonePort(ABC):
    @abstractmethod
    def save(self, key: str, content: dict[str]) -> dict:
        pass
