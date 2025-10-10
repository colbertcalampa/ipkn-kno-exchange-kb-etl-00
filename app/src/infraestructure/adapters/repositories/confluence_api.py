import requests
from requests.auth import HTTPBasicAuth
from typing import Dict

from app.src.application.ports.document_source_port import DocumentSourcePort

class ConfluenceAPIAdapter(DocumentSourcePort):
    def __init__(self, base_url: str, credentials: Dict[str, str]):
        self.base_url = base_url
        self.credentials = credentials
        self.headers = {"Accept": "application/json"}

    def _get_auth(self) -> HTTPBasicAuth:
        api_token = self.credentials.get("api_token")
        user_email = self.credentials.get("user_api_mail")

        if not api_token or not user_email:
            raise ValueError("Credenciales de Confluence incompletas")

        return HTTPBasicAuth(user_email, api_token)

    def get_page(self, page_id: str) -> Dict:
        url = f"{self.base_url}/rest/api/content/{page_id}?expand=body.storage,version,space"
        try:
            response = requests.get(url, headers=self.headers, auth=self._get_auth())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener la página {page_id} de Confluence: {e}")
            raise ValueError(f"Error al obtener la página {page_id} de Confluence: {e}")