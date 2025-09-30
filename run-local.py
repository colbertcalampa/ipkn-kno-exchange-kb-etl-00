import os
from app.src.entrypoints.etl_process.handler import handler

# Configurar variables de entorno para prueba local
os.environ['TABLE_NAME'] = 'dyn-security-poc'
os.environ['AWS_REGION'] = 'us-east-1'

# Mock del contexto Lambda para pruebas locales
class MockContext:
    def __init__(self):
        self.function_name = 'test-function'
        self.function_version = '1'
        self.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:test-function'
        self.memory_limit_in_mb = '128'
        self.remaining_time_in_millis = lambda: 30000
        self.log_group_name = '/aws/lambda/test-function'
        self.log_stream_name = '2023/01/01/[$LATEST]test'
        self.aws_request_id = 'test-request-id'

# Evento de prueba
event = {
  "headers": {
    "Content-Type": "application/json"
  },
  "body": "{\"page_id\": \"1732083713\", \"event_type\": \"updated\"}"
}

# Ejecutar handler con contexto mock
context = MockContext()
result = handler(event, context)
print(result)