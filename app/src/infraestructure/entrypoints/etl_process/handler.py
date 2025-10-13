import sys
sys.path.append('./lib')

import json
import os
import logging
import uuid
from typing import Any, Dict

from aws_lambda_powertools import Logger, Tracer

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.application.usecases.etl_process_use_case import ProcessUseCase

from app.src.shared.logger.powertools_logger import IOLambdaLogger

from app.src.infraestructure.adapters.repositories.confluence_api import ConfluenceAPIAdapter
from app.src.infraestructure.adapters.repositories.s3_repository import S3RepositoryAdapter
from app.src.infraestructure.adapters.etls.step_function_trigger import StepFunctionTriggerAdapter
from app.src.infraestructure.adapters.repositories.secrets_manager_adapter import SecretsManagerAdapter

io_logger = IOLambdaLogger(
    service=os.getenv("SERVICE", "io-crch-session-dial-cmd-01"),
    environment=os.getenv("ENV", "dev"),
    log_level=os.getenv("LOG_LEVEL"),
    service_version=os.getenv("SERVICE_VERSION", "$LATEST"),
)
logger = io_logger.get_logger()
tracer = Tracer()
settings_parameters = os.getenv("SESSION_DIAL_SETTINGS_PARAMETERS", "")


CONFLUENCE_SECRET_NAME = os.getenv("CONFLUENCE_SECRET_NAME", "sm-io-ipkn-kno-exchange-confluence-00")
CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "https://matrixmvp.atlassian.net/wiki")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")

AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", "colbert-test")
AWS_S3_BUCKET_PATH = os.getenv("AWS_S3_BUCKET_PATH", "s3-io-ipkn-kno-exchange_landing-00/landing")

AWS_STATE_MACHINE_ARN = os.getenv("AWS_S3_BUCKET_NAME", "arn:aws:states:us-east-1:627912843016:stateMachine:sfn-io-ipkn-kno-exchange-mngt-etl_process-00")


def _make_use_case() -> ProcessUseCase:
    secret_manager = SecretsManagerAdapter(AWS_REGION_NAME)
    confluence_credential_api = secret_manager.get_secret(CONFLUENCE_SECRET_NAME)
    return ProcessUseCase(
        ConfluenceAPIAdapter(CONFLUENCE_BASE_URL, confluence_credential_api),
        S3RepositoryAdapter(AWS_S3_BUCKET_NAME, AWS_S3_BUCKET_PATH, AWS_REGION_NAME),
        StepFunctionTriggerAdapter(AWS_STATE_MACHINE_ARN, AWS_REGION_NAME)
    )

@tracer.capture_lambda_handler
@logger.inject_lambda_context(log_event=True)
def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    print(f" event: {event}")
    logger.info(f" event: {event}")

    try:
        request_id = context.aws_request_id
        logger.info(f" RUN : ETL PROCESS DOCUMENT : REQUEST ID ({request_id})")

        body = json.loads(event.get("body") or "{}")
        page_id = body.get("page_id")
        event_type = body.get("event_type")

        document_event = DocumentEvent.from_process(page_id, event_type)

        use_case = _make_use_case()
        result = use_case.process(document_event)

        logger.info(f" RUN : ETL PROCESS DOCUMENT : END USE CASE")

        response_body = {
            "page_id": result.document_id,
            "event_type": result.event_type.value,
            "object_key": result.document_uri,
            "status": "OK",
            "request_id": request_id
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
