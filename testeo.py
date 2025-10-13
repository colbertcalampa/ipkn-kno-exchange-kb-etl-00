# app/src/infraestructure/entrypoints/etl_extract/handler.py

import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.application.usecases.etl_extract_use_case import ExtractDocumentUseCase

from app.src.infraestructure.adapters.sources.confluence_page_extractor_adapter import ConfluencePageExtractorAdapter
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

# Confluence (si aplica)
CONFLUENCE_SECRET_NAME = os.getenv("CONFLUENCE_SECRET_NAME")
CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL")


def _parse_event_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Admite eventos directos o API Gateway.
    """
    if "body" in event:
        body = event["body"]
        if isinstance(body, str):
            try:
                return json.loads(body) if body else {}
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON body: {exc}") from exc
        elif isinstance(body, dict):
            return body
        else:
            raise ValueError("Unsupported body type")
    return event  # asume invocación directa con el dict esperado


def _make_use_case() -> ExtractDocumentUseCase:
    source_storage = S3RepositoryAdapter(
        bucket=AWS_S3_LANDING_BUCKET,
        prefix=AWS_S3_LANDING_PREFIX,
        region_name=AWS_REGION_NAME,
    )

    target_storage = S3RepositoryAdapter(
        bucket=AWS_S3_GROUND_BUCKET,
        prefix=AWS_S3_GROUND_PREFIX,
        region_name=AWS_REGION_NAME,
    )

    document_extractor = ConfluencePageExtractorAdapter(
        base_url=CONFLUENCE_BASE_URL,
        secret_name=CONFLUENCE_SECRET_NAME,
        region_name=AWS_REGION_NAME,
    )

    return ExtractDocumentUseCase(
        source_storage=source_storage,
        document_extractor=document_extractor,
        target_storage=target_storage,
    )


def handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    correlation_id = getattr(context, "aws_request_id", None) or str(uuid.uuid4())

    try:
        logger.info("RUN: ETL EXTRACT DOCUMENT", extra={"correlation_id": correlation_id})

        body = _parse_event_payload(event)

        document_id = body.get("document_id")
        document_uri = body.get("document_uri")
        event_type_raw = body.get("event_type")

        if not document_id or not document_uri:
            raise ValueError("Required parameters are missing: document_id or document_uri")

        try:
            event_type = DocumentEventType(event_type_raw) if event_type_raw else None
        except Exception:
            raise ValueError(f"Invalid event_type: {event_type_raw}")

        if not event_type:
            raise ValueError("Required parameter is missing: event_type")

        document_event = DocumentEvent(document_id=document_id, event_type=event_type, document_uri=document_uri)

        use_case = _make_use_case()
        result = use_case.extract_document(document_event, correlation_id=correlation_id)

        response_body = {
            "document_id": result.document_id,
            "event_type": result.event_type.value if hasattr(result.event_type, "value") else result.event_type,
            "content_object_key": result.document_uri,
            "status": "OK",
            "correlation_id": correlation_id,
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "X-Correlation-Id": correlation_id,
            },
            "body": json.dumps(response_body),
        }

    except ValueError as e:
        logger.warning(
            "Bad request processing event",
            extra={"correlation_id": correlation_id, "error": str(e), "event": event},
        )
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json", "X-Correlation-Id": correlation_id},
            "body": json.dumps({"error": str(e), "correlation_id": correlation_id}),
        }
    except Exception as e:
        logger.error(
            "Unhandled error processing event",
            extra={"correlation_id": correlation_id, "error": str(e), "event": event, "error_type": type(e).__name__},
        )
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "X-Correlation-Id": correlation_id},
            "body": json.dumps({"error": "Internal server error", "correlation_id": correlation_id}),
        }