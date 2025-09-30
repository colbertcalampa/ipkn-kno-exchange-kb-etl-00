from app.src.domain.ports.confluence_port import ConfluencePort

class ConfluenceAPIAdapter(ConfluencePort):

    def get_page(self, page_id: str) -> dict:
        return {'page_id': page_id}
