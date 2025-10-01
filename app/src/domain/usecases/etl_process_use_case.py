import json

from app.src.domain.services.document_processor import DocumentProcessor

class EtlProcess:
    def __init__(self, document_source_port, landing_zone_port, recourse_trigger_port):
        self.processor = DocumentProcessor(
            document_source_port,
            landing_zone_port,
            recourse_trigger_port
        )

    def handle_event(self, document_event):

        if (document_event.page_id is None) or (document_event.event_type.value is None):
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Event without necessary data"}),
            }

        return self.processor.process(document_event)
