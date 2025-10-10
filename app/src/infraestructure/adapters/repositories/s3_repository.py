import boto3
import json

from app.src.application.ports.landing_zone_port import LandingZonePort


class S3RepositoryAdapter(LandingZonePort):
    def __init__(self, bucket, path, region_name):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket = bucket
        self.path = path
        self.content_type = 'application/json'

    def save(self, object_file_name: str, page_data: dict) -> dict:

        try:
            object_key = f"{self.path}/{object_file_name}"
            response = self.s3_client.put_object(Bucket=self.bucket, Key=object_key, Body=json.dumps(page_data),
                                                 ContentType=self.content_type)

            print(f"Archivo '{object_key}' guardado en bucket '{self.bucket}'.")

            return {
                "path": f"s3://{self.bucket}/{object_key}",
                "request_id": response.get("ResponseMetadata").get("RequestId")
            }

        except Exception as e:
            print(f"Error al subir archivo a S3: {e}")
            raise ValueError(f"Error al subir archivo a S3: {e}")
