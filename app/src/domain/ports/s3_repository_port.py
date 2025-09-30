from abc import ABC, abstractmethod


class S3RepositoryPort(ABC):
    @abstractmethod
    def save(self, page_id: str, page_data: dict[str]) -> dict:
        pass
