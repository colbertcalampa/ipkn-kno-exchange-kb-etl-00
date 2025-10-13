import logging

from datetime import datetime
from zoneinfo import ZoneInfo
from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.domain.model.process_result_event import ProcessResult
from app.src.application.ports.document_source_port import DocumentSourcePort
from app.src.application.ports.landing_zone_port import LandingZonePort
from app.src.application.ports.recourse_trigger_port import RecourseTriggerPort

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ProcessUseCase:
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

    def _get_page_data(self, event: DocumentEvent) -> dict:
        if event.event_type == DocumentEventType.UPDATED:
            logger.info("- Obteniendo documento desde fuente de datos ")
            return self.document_source.get_page(event.document_id)
        elif event.event_type == DocumentEventType.DELETED:
            logger.info("- Generando documento log eliminado ")
            return {"page_id": event.document_id, "event_type": event.event_type.value}
        else:
            raise ValueError(f"Unsupported event type: {event.event_type}")

    def process(self, event: DocumentEvent) -> ProcessResult:
        logger.info("Iniciando proceso ETL (ingesta y trigger)")

        if not event.document_id or not event.event_type:
            raise ValueError("Event without necessary data")

        logger.info(f"- Documento id : {event.document_id} ")
        logger.info(f"- Documento event type : {event.event_type.value} ")

        logger.info("- Obteniendo documento según evento")
        page_data = self._get_page_data(event)

        logger.info("- Generando nombre de objeto para almacenamiento")
        object_key = self._build_object_key(event.document_id, event.event_type)

        logger.info("- Carga de documento en repositorio landing")
        object_saved = self.landing_zone.save(object_key, page_data)

        logger.info(f"- Documento URI : {object_saved['uri']} ")

        logger.info("- Iniciando workflow de extraccion de datos")
        self.workflow_trigger.trigger(event.document_id, event.event_type.value, object_saved["uri"])

        return ProcessResult(
            event.document_id,
            event.event_type,
            object_key
        )
