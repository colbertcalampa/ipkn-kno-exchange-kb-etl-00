from enum import Enum

class DocumentEventType(Enum):
    UPDATED = "updated"
    DELETED = "deleted"


class DocumentEvent:
    def __init__(self, page_id: str, event_type: DocumentEventType):
        self.page_id = page_id
        self.event_type = event_type