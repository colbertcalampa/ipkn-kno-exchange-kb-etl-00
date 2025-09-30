
from domain.document_processor import DocumentProcessor
from adapters.repositories.confluence_api import ConfluenceAPIAdapter
from adapters.repositories.s3_repository import S3RepositoryAdapter
from adapters.repositories.step_function_trigger import StepFunctionTriggerAdapter

class EtlProcess:
    def __init__(self):
        self.processor = DocumentProcessor(
            ConfluenceAPIAdapter(),
            S3RepositoryAdapter(),
            StepFunctionTriggerAdapter()
        )

    def handle_event(self, document_event):
        return self.processor.process(document_event)
