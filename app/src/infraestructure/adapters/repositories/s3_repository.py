import boto3
import json

from urllib.parse import urlparse

from app.src.application.ports.landing_zone_port import LandingZonePort


# from app.src.application.ports. Ground


class S3RepositoryAdapter(LandingZonePort):

    def __init__(self, bucket, path, region_name):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket = bucket
        self.path = path
        self.content_type = 'application/json'

    def save(self, object_file_name: str, page_data: dict) -> dict:

        try:
            object_key = f"{self.path}/{object_file_name}"
            response = self.s3_client.put_object(Bucket=self.bucket, Key=object_key, Body=json.dumps(page_data, ensure_ascii=False).encode("utf-8"),
                                                 ContentType=self.content_type)

            print(f"Archivo '{object_key}' guardado en bucket '{self.bucket}'.")

            return {
                "uri": f"s3://{self.bucket}/{object_key}",
                "request_id": response.get("ResponseMetadata").get("RequestId")
            }

        except Exception as e:
            print(f"Error al subir archivo a S3: {e}")
            raise ValueError(f"Error al subir archivo a S3: {e}")

    def _parse_s3_uri(self, s3_uri):

        parsed = urlparse(s3_uri)
        if parsed.scheme != 's3':
            raise ValueError("El URI debe comenzar con 's3://'")

        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        return bucket, key

    def get_document(self, document_uri):
        try:
            bucket, key = self._parse_s3_uri(document_uri)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)

        except Exception as e:
            print(f"Error al leer archivo desde S3: {e}")
            raise ValueError(f"Error al leer archivo desde S3: {e}")
