from datetime import datetime
from zoneinfo import ZoneInfo

from app.src.domain.model.document_event import DocumentEventType

class DocumentProcessor:
    def __init__(self, document_source_port, landing_zone_port, recourse_trigger_port):
        self.document_source = document_source_port
        self.landing_zone = landing_zone_port
        self.recourse_trigger = recourse_trigger_port

    def process(self, event):
        if event.event_type.value == DocumentEventType.UPDATED.value:
            page_data = self.document_source.get_page(event.page_id)
            date_str = datetime.now(ZoneInfo("America/Lima")).strftime("%Y-%m-%d %H:%M:%S.%f")
            file_name = f"{event.page_id}_{date_str}.json"
            self.landing_zone.save(file_name, page_data)
            self.recourse_trigger.trigger(event.page_id, DocumentEventType.UPDATED.value)

        elif event.event_type.value == DocumentEventType.DELETED.value:
            page_data = {"page_id": event.page_id, "event_type": DocumentEventType.DELETED.value}
            self.landing_zone.save(event.page_id, page_data)
            self.recourse_trigger.trigger(event.page_id, DocumentEventType.DELETED.value)

        else:
            raise ValueError(f"Unsupported event type: {event.event_type.value}")

