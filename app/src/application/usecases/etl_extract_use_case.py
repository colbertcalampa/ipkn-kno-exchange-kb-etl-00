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


class ExtractDocumentUseCase:
    def __init__(self, landing_zone: LandingZonePort, extract_document, ground_truth_zone):
        self.landing_zone = landing_zone
        self.extract_document = extract_document
        self.ground_truth_zone = ground_truth_zone

    def extract_document(self, event: DocumentEvent):
        logger.info("Iniciando ETL extract document")

        if not event.document_id or not event.event_type:
            raise ValueError("Event without necessary data")

        logger.info(f"- Documento id : {event.document_id} ")
        logger.info(f"- Documento event type : {event.event_type.value} ")
        logger.info(f"- Documento uri : {event.document_uri} ")

        logger.info("- Obtención de documento desde landing")
        document_object = self.landing_zone.get_document_from_uri(event.document_uri)

        logger.info("- Extraccion data y metadata documento")
        document_data, document_metadata = self.extract_document.extract_content(document_object)

        data_object_key = f"{event.document_id}.html"
        metadata_object_key = f"{event.document_id}.metadata.html"

        logger.info("- Carga de documentos hacia zona ground truth")
        upload_data_saved = self.ground_truth_zone.save(data_object_key, document_data)
        upload_metadata_saved = self.ground_truth_zone.save(metadata_object_key, document_metadata)

        logger.info(f"- Documento data URI : {upload_data_saved['uri']} ")
        logger.info(f"- Documento metadata URI : {upload_metadata_saved['uri']} ")

        return ExtractResult(
            event.document_id,
            event.event_type,
            data_object_key
        )
