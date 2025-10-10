import boto3
import json

import unicodedata
import re
from bs4 import BeautifulSoup


class ExtractPageConfluenceAdapter:

    def __init__(self):
        print("")

    def split_html_content(self, page_body_value):
        page_soup_html = BeautifulSoup(page_body_value, "html.parser")
        cabecera_table_html = page_soup_html.find("table")
        cabecera_html_str = str(cabecera_table_html) if cabecera_table_html else ""
        if cabecera_table_html:
            cabecera_table_html.decompose()
        contenido_html_str = str(page_soup_html)
        return cabecera_html_str, contenido_html_str

    def extract_value_segun_header(self, header, cell_html):
        header_formated = header.strip()
        # print(f"cell_html : {cell_html}")
        if header_formated.lower() in ["autores", "autor"] + ["revisado por", "revisado"]:
            soup_cell = BeautifulSoup(str(cell_html), 'html.parser')
            user_tags = soup_cell.find_all('ri:user')
            cell_value = [tag['ri:account-id'] for tag in user_tags if tag.has_attr('ri:account-id')]
        elif header_formated.lower() in ["dominio"]:
            cell_value = cell_html.get_text(strip=True)
        elif header_formated.lower() in ["estado", "status"]:
            soup_cell = BeautifulSoup(str(cell_html), 'html.parser')
            status_tags = soup_cell.find_all('ac:parameter', {'ac:name': 'title'})
            cell_value = [tag_title.get_text(strip=True) for tag_title in status_tags]
        elif header_formated.lower() in ["fecha inicio vigencia", "fecha inicio"] + ["fecha fin vigencia", "fecha fin"]:
            cell_text = cell_html.get_text(strip=True)
            cell_text_uni = unicodedata.normalize("NFKD", cell_text)
            cell_text_re = re.sub(r'[\u200b\u202f\u00a0]', '', cell_text_uni)
            cell_value = cell_text_re.strip()

        else:
            cell_value = cell_html

        return cell_value

    def extract_cabecera_metadata(self, cabecera_html_str):
        cabecera_table_html = BeautifulSoup(cabecera_html_str, "html.parser")
        cabecera_table_metadata = {}
        if cabecera_table_html:
            for row in cabecera_table_html.find_all("tr"):
                headers = row.find_all("th")
                cells = row.find_all("td")
                if headers and cells:
                    headers_text = headers[0].get_text(strip=True)
                    cell_value = self.extract_value_segun_header(headers_text, cells[0])
                    cabecera_table_metadata[headers_text] = str(cell_value)
        return cabecera_table_metadata

    def extract_data(self, json_data: dict) -> tuple[dict, dict]:
        """
        Extrae metadatos generales y de filtro desde el JSON.
        Retorna una tupla: (metadata_general, metadata_filtro)
        """

        metadata_general = {
            "page_id": json_data.get("id", ""),
            "page_version_number": json_data.get("version", {}).get("number", ""),
            "page_status": json_data.get("status", ""),
            "page_title": json_data.get("title", "")
        }

        # metadata
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
        header, content = self.split_html_content(html_body)
        metadata_general |= self.extract_cabecera_metadata(header)

        return metadata_general, metadata_filtro
