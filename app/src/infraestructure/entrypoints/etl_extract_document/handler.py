import sys

sys.path.append('./lib')

import json
import os
import logging
import uuid
from typing import Any, Dict

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.application.usecases.etl_extract_use_case import ExtractDocumentUseCase

from app.src.infraestructure.adapters.transformer.extract_page_confluence_adapter import ExtractPageConfluenceAdapter
from app.src.infraestructure.adapters.repositories.s3_repository import S3RepositoryAdapter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Environment ---
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")

# Landing/source (desde donde se obtiene el documento)
AWS_S3_LANDING_BUCKET = os.getenv("AWS_S3_LANDING_BUCKET", "colbert-test")
AWS_S3_LANDING_PREFIX = os.getenv("AWS_S3_LANDING_PREFIX", "s3-io-ipkn-kno-exchange-landing-00")

# Ground truth/target (a donde se carga el resultado)
AWS_S3_GROUND_BUCKET = os.getenv("AWS_S3_GROUND_BUCKET", "colbert-test")
AWS_S3_GROUND_PREFIX = os.getenv("AWS_S3_GROUND_PREFIX", "s3-io-ipkn-kno-exchange-ground-truth-00/vigente")


def _make_use_case() -> ExtractDocumentUseCase:
    return ExtractDocumentUseCase(
        S3RepositoryAdapter(AWS_S3_LANDING_BUCKET, AWS_S3_LANDING_PREFIX, AWS_REGION_NAME),
        ExtractPageConfluenceAdapter(),
        S3RepositoryAdapter(AWS_S3_GROUND_BUCKET, AWS_S3_GROUND_PREFIX, AWS_REGION_NAME)
    )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    try:
        request_id = context.aws_request_id
        logger.info(f" RUN : ETL EXTRACT DOCUMENT : REQUEST ID ({request_id})" )

        body = event #json.loads(event.get("body") or "{}")
        document_id = body.get("document_id")
        event_type = body.get("event_type")
        document_uri = body.get("document_uri")

        if not document_id or not document_uri:
            raise ValueError("Required parameters are missing: document_id or document_uri")

        document_event = DocumentEvent(document_id, event_type, document_uri)

        use_case = _make_use_case()
        result = use_case.extract_document(document_event)

        logger.info(f" RUN : ETL EXTRACT DOCUMENT : END USE CASE")

        response_body = {
            "page_id": result.document_id,
            "event_type": result.event_type,
            "object_key": result.data_object_key,
            "status": "OK",
            "correlation_id": request_id,
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "X-Correlation-Id": request_id,
            },
            "body": json.dumps(response_body),
        }

    except Exception as e:
        logger.error(
            f"Error procesando evento: {str(e)}",
            extra={
                "event": event,
                "error_type": type(e).__name__
            },
        )
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
