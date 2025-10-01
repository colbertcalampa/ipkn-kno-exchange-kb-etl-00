from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from app.src.domain.model.document_event import DocumentEvent, DocumentEventType

from app.src.domain.ports.document_source_port import DocumentSourcePort
from app.src.domain.ports.landing_zone_port import LandingZonePort
from app.src.domain.ports.recourse_trigger_port import RecourseTriggerPort

@dataclass(frozen=True)
class ProcessResult:
    page_id: str
    event_type: DocumentEventType
    object_key: str  # clave usada en landing zone

class DocumentProcessor:
    def __init__(self,
                 document_source: DocumentSourcePort,
                 landing_zone: LandingZonePort,
                 workflow_trigger: RecourseTriggerPort):
        self.document_source = document_source
        self.landing_zone = landing_zone
        self.workflow_trigger = workflow_trigger

    def _build_object_key(self, page_id: str, event_type: DocumentEventType) -> str:

        ts = datetime.now(ZoneInfo("America/Lima")).strftime("%Y%m%dT%H%M%S-0500")
        match event_type:
            case DocumentEventType.UPDATED:
                return f"{page_id}_{ts}.json"
            case DocumentEventType.DELETED:
                return f"{page_id}_{ts}_deleted.json"
            case _:
                raise ValueError(f"Unsupported event type: {event_type}")

    def process(self, event: DocumentEvent) -> ProcessResult:
        match event.event_type:
            case DocumentEventType.UPDATED:
                page_data = self.document_source.get_page(event.page_id)
                object_key = self._build_object_key(event.page_id, event.event_type)
                self.landing_zone.save(object_key, page_data)
                #self.workflow_trigger.trigger(event.page_id, event.event_type, event.correlation_id)
                return ProcessResult(event.page_id, event.event_type, object_key)

            case DocumentEventType.DELETED:
                payload = {"page_id": event.page_id, "event_type": event.event_type.value}
                object_key = self._build_object_key(event.page_id, event.event_type)
                self.landing_zone.save(object_key, payload)
                #self.workflow_trigger.trigger(event.page_id, event.event_type, event.correlation_id)
                return ProcessResult(event.page_id, event.event_type, object_key)

            case _:
                raise ValueError(f"Unsupported event type: {event.event_type}")