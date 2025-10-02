import logging

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.domain.ports.document_source_port import DocumentSourcePort
from app.src.domain.ports.landing_zone_port import LandingZonePort
from app.src.domain.ports.recourse_trigger_port import RecourseTriggerPort

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@dataclass(frozen=True)
class ProcessResult:
    page_id: str
    event_type: DocumentEventType
    object_key: str

class EtlProcess:
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

    def handle_event(self, event: DocumentEvent) -> ProcessResult:

        logger.info("Iniciando proceso ETL (ingesta y trigger)")

        if not event.page_id or not event.event_type:
            raise ValueError("Event without necessary data")

        match event.event_type:
            case DocumentEventType.UPDATED:
                page_data = self.document_source.get_page(event.page_id)
            case DocumentEventType.DELETED:
                page_data = {"page_id": event.page_id, "event_type": event.event_type.value}
            case _:
                raise ValueError(f"Unsupported event type: {event.event_type}")

        logger.info("Datos de documento obtenidos Confluence/generados")

        object_key = self._build_object_key(event.page_id, event.event_type)
        object_saved = self.landing_zone.save(object_key, page_data)
        logger.info(f"Archivo {object_saved["path"]} subido a S3")

        self.workflow_trigger.trigger(event.page_id, event.event_type.value, object_saved["path"])
        logger.info("Step Function iniciado correctamente")

        return ProcessResult(event.page_id, event.event_type, object_key)