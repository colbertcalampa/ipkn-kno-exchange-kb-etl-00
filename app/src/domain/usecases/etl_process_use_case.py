from app.src.domain.services.document_processor import DocumentProcessor, ProcessResult
from app.src.domain.model.document_event import DocumentEvent

class EtlProcess:
    def __init__(self, document_source_port, landing_zone_port, workflow_trigger_port):
        self.processor = DocumentProcessor(
            document_source_port,
            landing_zone_port,
            workflow_trigger_port
        )

    def handle_event(self, document_event: DocumentEvent) -> ProcessResult:
        if not document_event.page_id or not document_event.event_type:
            raise ValueError("Event without necessary data")
        return self.processor.process(document_event)
