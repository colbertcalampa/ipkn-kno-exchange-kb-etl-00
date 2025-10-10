from abc import ABC, abstractmethod

from typing import Dict

class SecretManagerPort(ABC):
    @abstractmethod
    def get_secret(self, secret_name: str) -> Dict[str,str]:
        pass