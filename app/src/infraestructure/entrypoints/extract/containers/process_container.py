from typing import Any, Dict, Optional
from rodi import Container
from app.src.application.ports.converse.converse_mapper import ConverseMapperInterface
from app.src.application.ports.converse.converse_use_case import ConverseUseCaseInterface
from app.src.application.ports.shared.data_transformer import DataTransformerInterface
from app.src.application.ports.shared.logger_interface import LoggerInterface
from app.src.infraestructure.adapters.persistence.dyn_session_dial_events import DynSessionDialEventsRepository
from app.src.infraestructure.entrypoints.converse.settings.settings import Settings
from app.src.shared.helpers.data_transformer import DataTransformer
from app.src.application.ports.converse.session_dial_events_repository import (
    SessionDialEventsRepositoryInterface,
)
from app.src.application.ports.converse.converse_settings import SettingsInterface
from app.src.application.usecases.converse.converse_use_case import ConverseUseCase
from app.src.application.mappers.converse_mappers import ConverseMapper
from app.src.shared.logger.powertools_logger import IOLambdaLogger


def create_container(
    author: str,
    logger: IOLambdaLogger,
    settings: Optional[Dict[str, Any]] = None,
) -> Container:
    container = Container()

    settings_instance = Settings(author, settings or {})
    container.register(SettingsInterface, instance=settings_instance)

    # LOGGER - resolve with LoggerInterface
    def create_logger_child() -> LoggerInterface:
        return logger.create_child({"author": author, "loggerName": "Converse"})
    container.add_transient_by_factory(create_logger_child)

    # Helpers
    container.add_transient(DataTransformerInterface, DataTransformer)
    # Mappers
    container.add_transient(ConverseMapperInterface, ConverseMapper)
    # UseCases
    container.add_transient(ConverseUseCaseInterface, ConverseUseCase)

    # Repositories
    container.add_transient(
        SessionDialEventsRepositoryInterface, DynSessionDialEventsRepository
    )
    return container
