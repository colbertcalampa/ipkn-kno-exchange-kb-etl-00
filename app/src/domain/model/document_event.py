from enum import Enum

class DocumentEventType(Enum):
    UPDATED = "updated"
    DELETED = "deleted"

class DocumentEvent:
    def __init__(self, document_id: str, event_type: DocumentEventType, document_uri: str = ""):
        self.document_id = document_id
        self.event_type = event_type
        self.document_uri = document_uri