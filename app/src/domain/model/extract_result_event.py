from dataclasses import dataclass

from app.src.domain.model.document_event import DocumentEventType

@dataclass(frozen=True)
class ExtractResult:
    page_id: str
    event_type: DocumentEventType
    object_key: str
