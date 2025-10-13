from dataclasses import dataclass

from app.src.domain.model.document_event import DocumentEventType

@dataclass(frozen=True)
class ProcessResult:
    document_id: str
    event_type: DocumentEventType
    document_uri: str
