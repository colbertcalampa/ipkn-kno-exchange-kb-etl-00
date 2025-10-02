import boto3
import json

from app.src.domain.ports.recourse_trigger_port import RecourseTriggerPort
from typing import Any


class StepFunctionTriggerAdapter(RecourseTriggerPort):

    def __init__(self, state_machines_arn, region_name):
        self.step_functions_client = boto3.client('stepfunctions', region_name=region_name)
        self.state_machines_arn = state_machines_arn

    def trigger(self, page_id: str, event_type: str, object_key: str) -> Any:

        try:
            input_step_function = {
                "state_input":
                    {
                        "page_id": page_id,
                        "event_type": event_type,
                        "object_key": object_key
                    }
            }

            response = self.step_functions_client.start_execution(
                stateMachineArn=self.state_machines_arn,
                input=json.dumps(input_step_function)
            )
            print(f" Step function response : {response}")

            return response
        except Exception as e:

            print(f"Error al iniciar step function :{self.state_machines_arn}: {e}")
            raise ValueError(f"Error al iniciar step function :{self.state_machines_arn}: {e}")
