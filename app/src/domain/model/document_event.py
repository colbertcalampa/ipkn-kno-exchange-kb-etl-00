from enum import Enum
from dataclasses import dataclass

class DocumentEventType(Enum):
    UPDATED = "updated"
    DELETED = "deleted"

@dataclass(frozen=True)
class DocumentEvent:
    """
    Representa un evento de documento para el proceso ETL.
    """
    document_id: str
    event_type: DocumentEventType
    document_uri: str = ""

    @staticmethod
    def _parse_event_type(event_type: str) -> DocumentEventType:
        try:
            return DocumentEventType(event_type)
        except ValueError:
            raise ValueError(f"event_type inválido: {event_type}")

    @classmethod
    def from_process(cls, document_id: str, event_type: str):
        if not document_id:
            raise ValueError("document_id es requerido")
        return cls(document_id, cls._parse_event_type(event_type))

    @classmethod
    def from_extract(cls, document_id: str, event_type: str, document_uri: str):
        if not document_id or not document_uri:
            raise ValueError("document_id y document_uri son requeridos")
        return cls(document_id, cls._parse_event_type(event_type), document_uri)
