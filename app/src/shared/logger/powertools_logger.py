from logging import LogRecord
import os
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging.formatter import LambdaPowertoolsFormatter
from aws_xray_sdk.core import xray_recorder

from app.src.application.ports.shared.logger_interface import LoggerInterface


class Environment:
    DEV = "dev"
    QA = "qa"
    UAT = "uat"
    PROD = "prod"


class LogLevel:
    DEBUG = "DEBUG"
    INFO = "INFO"
    ERROR = "ERROR"
    SILENT = "SILENT"


def determinate_log_level(
    environment: str, log_level_name: Optional[str] = None
) -> str:
    env = environment.lower()
    if env in [Environment.UAT, Environment.PROD]:
        return LogLevel.INFO

    if env in [Environment.DEV, Environment.QA] and not log_level_name:
        return LogLevel.SILENT
    if log_level_name:
        level_upper = log_level_name.upper()
        valid_levels = [
            LogLevel.DEBUG,
            LogLevel.INFO,
            LogLevel.SILENT,
            LogLevel.ERROR,
            LogLevel.SILENT,
        ]
        if level_upper in valid_levels:
            return level_upper

    return LogLevel.SILENT


class CustomPowertoolsFormatter(LambdaPowertoolsFormatter):

    def __init__(self, service: str, author: str = ""):
        super().__init__(
            log_record_order=["level", "location", "message", "timestamp"],
            utc=True,
            use_rfc3339=True,
        )
        self.author = author
        self.service = service

    def set_author(self, author: str) -> None:
        self.author = author

    def format(self, record: LogRecord) -> str:
        formatted_log_str = super().format(record)

        try:
            powertools_data = json.loads(formatted_log_str)
        except:
            powertools_data = {}

        custom_event = {
            "message": powertools_data.get("message", record.getMessage()),
            "xRayTraceId": powertools_data.get("xray_trace_id"),
            "timestamp": powertools_data.get(
                "timestamp", datetime.utcnow().isoformat() + "Z"
            ),
            "level": powertools_data.get("level", record.levelname.lower()),
            "env": os.getenv("ENV", "dev"),
            "serviceVersion": os.getenv("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST"),
            "service": powertools_data.get("service", self.service),
            "requestId": powertools_data.get("function_request_id", ""),
            "functionInfo": {
                "memoryLimitInMB": powertools_data.get(
                    "function_memory_size",
                    os.getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "256"),
                ),
                "coldStart": powertools_data.get("cold_start", True),
                "version": os.getenv("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST"),
                "handler": powertools_data.get(
                    "function_name", os.getenv("AWS_LAMBDA_FUNCTION_NAME", "")
                ),
            },
            "loggerName": "Logger",
            "author": self.author,
        }
        extra_fields = {
            k: v
            for k, v in powertools_data.items()
            if k
            not in [
                "level",
                "location",
                "message",
                "timestamp",
                "service",
                "function_request_id",
                "function_memory_size",
                "cold_start",
                "function_name",
                "xray_trace_id",
                "exception",
                "exception_name",
                "stack_trace",
            ]
        }
        custom_event.update(extra_fields)

        return json.dumps(custom_event)


class IOLambdaLogger(LoggerInterface):

    def __init__(
        self,
        service: str,
        environment: Optional[str] = None,
        log_level: Optional[str] = None,
        service_version: Optional[str] = None,
    ):
        self.service = service
        self.environment = environment or os.getenv("ENV", "dev")
        self.service_version = service_version or "$LATEST"

        self._logger = Logger(service=service)
        self._tracer = Tracer()

        self.set_log_level(self.environment, log_level or os.getenv("LOG_LEVEL"))

        self.custom_formatter = CustomPowertoolsFormatter(service)
        for handler in self._logger.handlers:
            handler.setFormatter(self.custom_formatter)

    def set_author(self, author: str) -> None:
        self.custom_formatter.set_author(author)

    def set_log_level(
        self, environment: str, log_level_name: Optional[str] = None
    ) -> None:

        level_name = determinate_log_level(environment, log_level_name)
        level_mapping = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.SILENT: logging.CRITICAL,
        }

        log_level = level_mapping.get(level_name, logging.INFO)
        self._logger.setLevel(log_level)

        for handler in self._logger.handlers:
            handler.setLevel(log_level)

    def get_transaction_id(self) -> Optional[str]:
        try:
            xray_trace_id = os.getenv("_X_AMZN_TRACE_ID")
            if xray_trace_id:
                return xray_trace_id.split(";")[0].replace("Root=", "")

            trace_entity = xray_recorder.current_segment()
            if trace_entity and hasattr(trace_entity, "trace_id"):
                return trace_entity.trace_id

            trace_id = self._tracer.get_trace_id()
            if trace_id:
                return trace_id

            return None
        except Exception as e:
            self._logger.debug(f"Error getting transaction ID: {str(e)}")
            return None

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.info(message, extra=extra or {})

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.error(message, extra=extra or {})

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.warning(message, extra=extra or {})

    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.debug(message, extra=extra or {})

    def exception(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.exception(message, extra=extra or {})

    def get_logger(self) -> Logger:
        return self._logger

    def append_keys(self, **additional_keys) -> None:
        self._logger.append_keys(**additional_keys)

    def remove_keys(self, keys) -> None:
        self._logger.remove_keys(keys)

    def create_child(
        self, persistent_log_attributes: Optional[Dict[str, Any]] = None
    ) -> "IOLambdaLogger":

        child = IOLambdaLogger(
            service=self.service,
            environment=self.environment,
            log_level=None,
            service_version=self.service_version,
        )

        child._logger.setLevel(self._logger.level)
        for handler in child._logger.handlers:
            handler.setLevel(self._logger.level)

        if persistent_log_attributes:
            if "author" in persistent_log_attributes:
                child.set_author(persistent_log_attributes["author"])

            other_attrs = {
                k: v for k, v in persistent_log_attributes.items() if k != "author"
            }
            if other_attrs:
                child.append_keys(**other_attrs)

        return child
