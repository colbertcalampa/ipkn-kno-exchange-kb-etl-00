from abc import ABC, abstractmethod

from app.src.domain.converse.types.converse_input import ConverseInput


class ProcessUseCaseInterface(ABC):
    @abstractmethod
    def process(self, input: ConverseInput):
        pass
