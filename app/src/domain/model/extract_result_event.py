from dataclasses import dataclass

from app.src.domain.model.document_event import DocumentEventType

@dataclass(frozen=True)
class ExtractResult:
    document_id: str
    event_type: DocumentEventType
    data_object_key: str
