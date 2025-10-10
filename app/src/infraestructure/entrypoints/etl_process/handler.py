import sys
sys.path.append('./lib')

import json
import os
import logging
import uuid
from typing import Any, Dict

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.application.usecases.etl_process_use_case import EtlProcess

from app.src.infraestructure.adapters.repositories.confluence_api import ConfluenceAPIAdapter
from app.src.infraestructure.adapters.repositories.s3_repository import S3RepositoryAdapter
from app.src.infraestructure.adapters.etls.step_function_trigger import StepFunctionTriggerAdapter
from app.src.infraestructure.adapters.repositories.secrets_manager_adapter import SecretsManagerAdapter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CONFLUENCE_SECRET_NAME = os.getenv("CONFLUENCE_SECRET_NAME", "sm-io-ipkn-kno-exchange-confluence-00")
CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "https://matrixmvp.atlassian.net/wiki")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")

AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", "colbert-test")
AWS_S3_BUCKET_PATH = os.getenv("AWS_S3_BUCKET_PATH", "s3-io-ipkn-kno-exchange_landing-00/landing")

AWS_STATE_MACHINE_ARN = os.getenv("AWS_S3_BUCKET_NAME", "arn:aws:states:us-east-1:627912843016:stateMachine:sfn-io-ipkn-kno-exchange-mngt-etl_process-00")

def _make_use_case() -> EtlProcess:
    secret_manager = SecretsManagerAdapter(AWS_REGION_NAME)
    confluence_credential_api = secret_manager.get_secret(CONFLUENCE_SECRET_NAME)
    return EtlProcess(
        ConfluenceAPIAdapter(CONFLUENCE_BASE_URL, confluence_credential_api),
        S3RepositoryAdapter(AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PATH, AWS_REGION_NAME),
        StepFunctionTriggerAdapter(AWS_STATE_MACHINE_ARN, AWS_REGION_NAME)
    )

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    correlation_id = (event.get("headers") or {}).get("X-Correlation-Id") or str(uuid.uuid4())
    try:
        logger.info("Iniciando proceso ETL Confluence → S3 AWS", extra={"correlation_id": correlation_id})

        body = json.loads(event.get("body") or "{}")
        page_id = body.get("page_id")
        event_type_str = body.get("event_type")

        if not page_id or not event_type_str:
            raise ValueError("Required parameters are missing: page_id or event_type")

        try:
            event_type = DocumentEventType(event_type_str)
        except ValueError:
            raise ValueError(f"Invalid event_type: {event_type_str}")

        use_case = _make_use_case()
        result = use_case.handle_event(DocumentEvent(page_id, event_type))

        response_body = {
            "page_id": result.page_id,
            "event_type": result.event_type.value,
            "object_key": result.object_key,
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

    except Exception as e:
        logger.error(
            f"Error procesando evento: {str(e)}",
            extra={
                "event": event,
                "error_type": type(e).__name__
            },
        )
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
