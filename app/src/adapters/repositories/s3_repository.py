from app.src.domain.ports.s3_repository_port import S3RepositoryPort

class S3RepositoryAdapter(S3RepositoryPort):

    def save(self, page_id: str, page_data: dict[str]) -> dict:
        print(f"Pagina {page_id} cargada en s3 ")
