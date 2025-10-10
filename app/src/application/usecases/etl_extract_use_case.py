import logging

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.application.ports.document_source_port import DocumentSourcePort
from app.src.application.ports.landing_zone_port import LandingZonePort
from app.src.application.ports.recourse_trigger_port import RecourseTriggerPort

from app.src.domain.model.extract_result_event import ExtractResult

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class EtlExtract:
    def __init__(self,
                 landing_zone: LandingZonePort):
        self.landing_zone = landing_zone
        self.extract_document = landing_zone # PEND
        self.ground_truth_zone = landing_zone

    def handle_event(self, event: DocumentEvent) :

        logger.info("Iniciando ETL extract document")

        if not event.page_id or not event.event_type:
            raise ValueError("Event without necessary data")

        logger.info("- Descarga de documento desde landing")
        document_object = self.landing_zone.download_data(object_key)

        logger.info("- Extraccion data y metadata documento")
        document_data, document_metadata = self.extract_document.extract_data(document_object)

        logger.info("- Carga de documento hacia ground truth")
        upload_response = self.ground_truth_zone.upload_data(document_data, document_metadata)

        return ExtractResult(event.page_id, event.event_type, object_key)