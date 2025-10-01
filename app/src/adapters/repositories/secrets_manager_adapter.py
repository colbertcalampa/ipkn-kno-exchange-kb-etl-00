import boto3
import json


from app.src.domain.ports.secret_manager_port import SecretManagerPort


class SecretsManagerAdapter(SecretManagerPort):
    def __init__(self, region_name):
        self.secrets_manager_client = boto3.client('secretsmanager' , region_name=region_name)

    def get_secret(self, secret_name: str) -> dict:
        try:
            response = self.secrets_manager_client.get_secret_value(SecretId=secret_name)
            secret_string = response.get('SecretString', '{}')
            return json.loads(secret_string)
        except Exception as e:
            print(f"Error getting secret: {type(e).__name__}")
            return {}

