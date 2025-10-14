from typing import Any, Dict, Optional
from rodi import Container

from app.src.application.ports.process.process_use_case import ProcessUseCaseInterface

from app.src.application.ports.document_source_port import DocumentSourceInterface
from app.src.application.ports.landing_zone_port import LandingZoneInterface
from app.src.application.ports.recourse_trigger_port import RecourseTriggerInterface
from app.src.application.ports.secret_manager_port import SecretManagerInterface

from app.src.application.ports.shared.logger_interface import LoggerInterface
from app.src.shared.logger.powertools_logger import IOLambdaLogger

from app.src.application.usecases.process_use_case import ProcessUseCase

from app.src.infraestructure.adapters.repositories.confluence_api import ConfluenceAPIAdapter
from app.src.infraestructure.adapters.repositories.s3_repository import S3RepositoryAdapter
from app.src.infraestructure.adapters.etls.step_function_trigger import StepFunctionTriggerAdapter
from app.src.infraestructure.adapters.repositories.secrets_manager_adapter import SecretsManagerAdapter


def create_container(
        author: str,
        logger: IOLambdaLogger,
        config_resource: Dict[str, str]
) -> Container:
    container = Container()

    # LOGGER - resolve with LoggerInterface
    def create_logger_child() -> LoggerInterface:
        return logger.create_child({"author": author, "loggerName": "Process"})

    container.add_transient_by_factory(create_logger_child)

    def confluence_api_factory() -> DocumentSourceInterface:
        secret_manager = SecretsManagerAdapter(config_resource["aws_region_name"])
        confluence_credential_api = secret_manager.get_secret(config_resource["confluence_secret_name"])
        return ConfluenceAPIAdapter(config_resource["confluence_base_url"], confluence_credential_api)

    container.add_transient_by_factory(confluence_api_factory, DocumentSourceInterface)

    def s3_landing_factory() -> LandingZoneInterface:
        return S3RepositoryAdapter(config_resource["aws_s3_bucket_name"], config_resource["aws_s3_bucket_path"],
                                   config_resource["aws_region_name"])

    container.add_transient_by_factory(s3_landing_factory, LandingZoneInterface)

    def step_function_factory() -> RecourseTriggerInterface:
        return StepFunctionTriggerAdapter(config_resource["aws_state_machine_arn"],
                                          config_resource["aws_region_name"])

    container.add_transient_by_factory(step_function_factory, RecourseTriggerInterface)

    # Use Cases
    container.add_transient(
        ProcessUseCaseInterface, ProcessUseCase
    )
    return container
