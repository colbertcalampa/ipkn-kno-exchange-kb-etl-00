import sys
sys.path.append('./lib')

import json
import os
import logging
from typing import Any, Dict

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.domain.usecases.etl_process_use_case import EtlProcess
from app.src.adapters.repositories.confluence_api import ConfluenceAPIAdapter
from app.src.adapters.repositories.s3_repository import S3RepositoryAdapter
from app.src.adapters.repositories.step_function_trigger import StepFunctionTriggerAdapter
from app.src.adapters.repositories.secrets_manager_adapter import SecretsManagerAdapter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONFLUENCE_SECRET_NAME = os.getenv("CONFLUENCE_SECRET_NAME", "sm-dev-ia-contact-handler-ke-ingest-01")
CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "https://matrixmvp.atlassian.net/wiki")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info("Iniciando proceso ETL Confluence → S3 AWS")

        body = json.loads(event.get("body", "{}"))
        page_id = body.get("page_id")
        event_type = body.get("event_type")

        if not page_id or not event_type:
            raise ValueError("Required parameters are missing: page_id or event_type")

        logger.info(f"Processing event: page_id={page_id}, event_type={event_type}")

        document_event = DocumentEvent(page_id, DocumentEventType(event_type))

        secret_manager = SecretsManagerAdapter(AWS_REGION_NAME)
        confluence_credential_api = secret_manager.get_secret(CONFLUENCE_SECRET_NAME)

        caso_uso = EtlProcess(
            ConfluenceAPIAdapter(CONFLUENCE_BASE_URL, confluence_credential_api),
            S3RepositoryAdapter(AWS_REGION_NAME),
            StepFunctionTriggerAdapter()
        )

        caso_uso.handle_event(document_event)

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'page_id': page_id, 'status': 'OK'})
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