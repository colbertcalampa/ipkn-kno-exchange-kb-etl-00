from domain.model.document_event import DocumentEventType

class DocumentProcessor:
    def __init__(self, confluence_adapter, s3_adapter, step_function_adapter):
        self.confluence = confluence_adapter
        self.s3 = s3_adapter
        self.step_function = step_function_adapter

    def process(self, event):
        if event.event_type.value == DocumentEventType.UPDATED.value:
            page_data = self.confluence.get_page(event.page_id)
            self.s3.save(event.page_id, page_data)
            self.step_function.trigger(event.page_id, DocumentEventType.UPDATED.value)

        elif event.event_type.value == DocumentEventType.DELETED.value:
            page_data = {"page_id": event.page_id, "event_type": DocumentEventType.DELETED.value}
            self.s3.save(event.page_id, page_data)
            self.step_function.trigger(event.page_id, DocumentEventType.DELETED.value)

        else:
            raise ValueError(f"Unsupported event type: {event.event_type.value}")
