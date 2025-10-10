import sys

sys.path.append('./lib')

import json
import os
import logging
import uuid
from typing import Any, Dict

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.application.usecases.etl_extract_use_case import EtlExtract

from app.src.infraestructure.adapters.transformer.extract_page_confluence_adapter import ExtractPageConfluenceAdapter
from app.src.infraestructure.adapters.repositories.s3_repository import S3RepositoryAdapter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CONFLUENCE_SECRET_NAME = os.getenv("CONFLUENCE_SECRET_NAME", "sm-io-ipkn-kno-exchange-confluence-00")
CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "https://matrixmvp.atlassian.net/wiki")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")

AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", "colbert-test")
AWS_S3_BUCKET_PATH = os.getenv("AWS_S3_BUCKET_PATH", "s3-io-ipkn-kno-exchange-ground-truth-00/vigente")


def _make_use_case() -> EtlExtract:
    return EtlExtract(
        S3RepositoryAdapter(AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PATH, AWS_REGION_NAME),
        ExtractPageConfluenceAdapter(),
        S3RepositoryAdapter(AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PATH, AWS_REGION_NAME)
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
            raise ValueError("Required parameters are missing: page_id or document_uri")

        document_event = DocumentEvent(document_id, event_type)
        document_event.document_uri = document_uri

        use_case = _make_use_case()
        result = use_case.handle_event(document_event)

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
