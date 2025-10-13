from bs4 import BeautifulSoup
import unicodedata
import re

class ExtractContentConfluenceAdapter:

    def split_html_content(self, page_body_value):
        soup = BeautifulSoup(page_body_value, "html.parser")
        table = soup.find("table")
        cabecera_html = str(table) if table else ""
        if table:
            table.decompose()
        contenido_html = str(soup)
        return cabecera_html, contenido_html

    def extract_value_segun_header(self, header, cell_html):
        header_lower = header.strip().lower()
        soup_cell = BeautifulSoup(str(cell_html), 'html.parser')
        if header_lower in {"autores", "autor", "revisado por", "revisado"}:
            return [tag['ri:account-id'] for tag in soup_cell.find_all('ri:user') if tag.has_attr('ri:account-id')]
        if header_lower == "dominio":
            return cell_html.get_text(strip=True)
        if header_lower in {"estado", "status"}:
            return [tag.get_text(strip=True) for tag in soup_cell.find_all('ac:parameter', {'ac:name': 'title'})]
        if header_lower in {"fecha inicio vigencia", "fecha inicio", "fecha fin vigencia", "fecha fin"}:
            text = cell_html.get_text(strip=True)
            text = unicodedata.normalize("NFKD", text)
            return re.sub(r'[\u200b\u202f\u00a0]', '', text).strip()
        return cell_html

    def extract_cabecera_metadata(self, cabecera_html_str):
        soup = BeautifulSoup(cabecera_html_str, "html.parser")
        metadata = {}
        for row in soup.find_all("tr"):
            headers = row.find_all("th")
            cells = row.find_all("td")
            if headers and cells:
                header_text = headers[0].get_text(strip=True)
                value = self.extract_value_segun_header(header_text, cells[0])
                metadata[header_text] = str(value)
        return metadata

    def extract_content(self, json_data: dict) -> tuple[dict, dict]:

        metadata_general = {
            "page_id": json_data.get("id", ""),
            "page_version_number": json_data.get("version", {}).get("number", ""),
            "page_status": json_data.get("status", ""),
            "page_title": json_data.get("title", "")
        }

        metadata_filtro = {
            "metadataAttributes": {
                "id_test": json_data.get("id", ""),
                "title": json_data.get("title", ""),
                "space": json_data.get("space", {}).get("key", ""),
                "spacename": json_data.get("space", {}).get("name", ""),
                "spacetype": json_data.get("space", {}).get("type", ""),
                "spacestatus": json_data.get("space", {}).get("status", ""),
                "base64EncodedAri": json_data.get("base64EncodedAri", ""),
                "ari": json_data.get("ari", ""),
                "type": json_data.get("type", ""),
                "status": json_data.get("status", "")
            }
        }

        html_body = json_data.get("body", {}).get("storage", {}).get("value", "")
        header, _ = self.split_html_content(html_body)
        metadata_general |= self.extract_cabecera_metadata(header)
        return metadata_general, metadata_filtro
