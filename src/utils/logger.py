"""Модуль для настройки системы логирования."""

import logging
import sys
from logging import FileHandler, StreamHandler
from pathlib import Path

from src.models.config import LoggingConfig


class Formatter(logging.Formatter):
    """Кастомный форматтер для логов.

    Определяет стандартный формат вывода лог-сообщений.
    """

    def __init__(self) -> None:
        """Инициализация параметров."""
        super().__init__(
            fmt="[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def _create_handler(
    handler_type: str,
    config: LoggingConfig,
    formatter: logging.Formatter,
    log_path: Path | None = None,
) -> StreamHandler | FileHandler:
    """Создает обработчик логов указанного типа.

    :param handler_type: тип обработчика ('console' или 'file')
    :type handler_type: str
    :param config: конфигурация логирования
    :type config: LoggingConfig
    :param formatter: форматтер для обработчика
    :type formatter: logging.Formatter
    :param log_path: путь к файлу лога (только для типа 'file')
    :type log_path: Path | None
    :return: созданный обработчик логов
    :rtype: StreamHandler | FileHandler
    :raises ValueError: если указан неверный тип обработчика
    """
    handler: StreamHandler | FileHandler
    if handler_type == "console":
        handler = StreamHandler(sys.stdout)
    elif handler_type == "file" and log_path:
        handler = FileHandler(
            filename=log_path,
            mode="a",
            encoding="utf-8",
        )
    else:
        error_message = f"Invalid handler type: {handler_type}"
        raise ValueError(error_message)

    handler.setLevel(config.level)
    handler.setFormatter(formatter)
    return handler


def _get_log_folder() -> Path:
    """Возвращает путь к папке для логов.

    :return: путь к папке логов
    :rtype: Path
    """
    return Path(__file__).parent.parent.parent.joinpath("log")


def _setup_handlers(config: LoggingConfig, formatter: logging.Formatter) -> list:
    """Настраивает обработчики логов в соответствии с конфигурацией.

    :param config: конфигурация логирования
    :type config: LoggingConfig
    :param formatter: форматтер для обработчиков
    :type formatter: logging.Formatter
    :return: список настроенных обработчиков
    :rtype: list
    """
    handlers = []

    if config.output in {"console", "both"}:
        console_handler = _create_handler("console", config, formatter)
        handlers.append(console_handler)

    if config.output in {"file", "both"}:
        log_folder = _get_log_folder()
        log_folder.mkdir(parents=True, exist_ok=True)
        log_path = log_folder / config.filename

        file_handler = _create_handler("file", config, formatter, log_path)
        handlers.append(file_handler)

    return handlers


def setup_logger(config: LoggingConfig | None) -> None:
    """Настраивает глобальную конфигурацию логирования.

    :param config: конфигурация логирования или None для отключения логирования
    :type config: LoggingConfig | None
    """
    if not config:
        logging.basicConfig(level=logging.CRITICAL + 1)
        return

    formatter = Formatter()
    handlers = _setup_handlers(config, formatter)

    logging.basicConfig(
        level=config.level,
        handlers=handlers,
        encoding="utf-8",
        force=True,
    )
