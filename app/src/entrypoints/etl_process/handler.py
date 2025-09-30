import json
import os
from typing import Any, Dict

from app.src.domain.model.document_event import DocumentEvent, DocumentEventType
from app.src.domain.usecases.etl_process_use_case import EtlProcess


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    try:
        logger.info("Iniciando process etl confluence > s3 aws")

        body = json.loads(event.get("body", "{}"))
        page_id = body.get("page_id", None)
        event_type = DocumentEventType(body.get("event_type", None))

        if page_id is None or event_type is None:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No page id or event type to process"}),
            }

        document_event = DocumentEvent(page_id, event_type)

        service = EtlProcess()
        service.handle_event(document_event)

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
            f"Error processing events: {str(e)}",
            extra={
                "event": event,
                "error_type": type(e).__name__
            },
        )
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}