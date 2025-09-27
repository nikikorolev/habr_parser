"""Модуль конфигурации парсера.

Содержит Pydantic модели для валидации и хранения конфигурационных параметров:
- PagesConfig: настройки диапазона страниц для парсинга
- SaveConfig: настройки сохранения результатов
- HeadersConfig: настройки HTTP-заголовков
- LoggingConfig: настройки логирования
- RequestConfig: настройки HTTP-запросов
- SessionConfig: настройки HTTP-сессии
- ParserConfig: основная конфигурация парсера
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class PagesConfig(BaseModel):
    """Конфигурация диапазона страниц для парсинга.

    :ivar first: номер первой страницы
    :vartype first: int
    :ivar last: номер последней страницы
    :vartype last: int
    """

    first: int
    last: int

    @field_validator("last", mode="before")
    @classmethod
    def check_last_not_less_first(cls, v: int, info: dict[str, Any]) -> int:
        """Проверяет, что last не меньше first.

        :param v: значение last для валидации
        :type v: int
        :param info: информация о валидации
        :type info: dict[str, Any]
        :return: проверенное значение last
        :rtype: int
        :raises ValueError: если last меньше first и first меньше 1
        """
        first_value = info.data.get("first")
        if v is not None and first_value is not None and v < first_value and first_value < 1:
            error_message = f"'last' ({v}) cannot be less than 'first' ({first_value})"
            raise ValueError(error_message)
        return v


class SaveConfig(BaseModel):
    """Конфигурация для сохранения результатов.

    :ivar file: имя файла для сохранения
    :vartype file: str
    :ivar path: путь для сохранения
    :vartype path: str
    :ivar extension: расширение файла
    :vartype extension: str
    :ivar skip: сохранять ли статьи с ошибками
    :vartype skip: bool
    """

    file: str
    path: str
    extension: str
    skip: bool

    @field_validator("extension", mode="before")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Валидирует допустимые типы файлов.

        :param v: расширение файла для валидации
        :type v: str
        :return: проверенное расширение файла
        :rtype: str
        :raises ValueError: если расширение не поддерживается
        """
        allowed_types = {"parquet", "csv", "json"}
        if v not in allowed_types:
            error_message = f"type must be one of {allowed_types}, got '{v}'"
            raise ValueError(error_message)
        return v

    def get_path(self) -> Path:
        """Формирует полный путь к файлу для сохранения.

        :return: полный путь к файлу
        :rtype: Path
        """
        return Path(self.path) / f"{self.file}.{self.extension}"


class HeadersConfig(BaseModel):
    """Конфигурация HTTP-заголовков.

    :ivar user_agent: User-Agent заголовок
    :vartype user_agent: str | None
    :ivar accept: Accept заголовок
    :vartype accept: str | None
    :ivar accept_language: Accept-Language заголовок
    :vartype accept_language: str | None
    :ivar accept_encoding: Accept-Encoding заголовок
    :vartype accept_encoding: str | None
    :ivar connection: Connection заголовок
    :vartype connection: str | None
    :ivar referer: Referer заголовок
    :vartype referer: str | None
    """

    user_agent: str | None = None
    accept: str | None = None
    accept_language: str | None = None
    accept_encoding: str | None = None
    connection: str | None = None
    referer: str | None = None

    def build_headers(self) -> dict[str, str | None]:
        """Собирает заголовки в словарь для HTTP-запросов.

        :return: словарь с HTTP-заголовками
        :rtype: dict[str, str | None]
        """
        return {
            "User-Agent": self.user_agent,
            "Accept": self.accept,
            "Accept-Language": self.accept_language,
            "Accept-Encoding": self.accept_encoding,
            "Connection": self.connection,
            "Referer": self.referer,
        }


class LoggingConfig(BaseModel):
    """Конфигурация логирования.

    :ivar level: уровень логирования
    :vartype level: str
    :ivar output: вывод логов (console, file, both)
    :vartype output: str
    :ivar filename: имя файла для логирования
    :vartype filename: str
    """

    level: str
    output: str
    filename: str | None = None

    @field_validator("level", mode="before")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Валидирует уровень логирования.

        :param v: уровень логирования для валидации
        :type v: str
        :return: проверенный уровень логирования
        :rtype: str
        :raises ValueError: если уровень не поддерживается
        """
        allowed_levels = {"NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v not in allowed_levels:
            error_message = f"level must be one of {allowed_levels}, got '{v}'"
            raise ValueError(error_message)
        return v

    @field_validator("output", mode="before")
    @classmethod
    def validate_output(cls, v: str) -> str:
        """Валидирует тип вывода логов.

        :param v: тип вывода для валидации
        :type v: str
        :return: проверенный тип вывода
        :rtype: str
        :raises ValueError: если тип вывода не поддерживается
        """
        allowed_outputs = {"console", "file", "both"}
        if v not in allowed_outputs:
            error_message = f"level must be one of {allowed_outputs}, got '{v}'"
            raise ValueError(error_message)
        return v


class SessionConfig(BaseModel):
    """Конфигурация HTTP-сессии.

    :ivar force_close: принудительное закрытие соединения
    :vartype force_close: bool
    :ivar limit: максимальное количество соединений
    :vartype limit: int
    :ivar limit_per_host: максимальное количество соединений на хост
    :vartype limit_per_host: int
    :ivar ttl_dns_cache: TTL DNS кэша
    :vartype ttl_dns_cache: int | None
    """

    force_close: bool
    limit: int = Field(..., gt=0)
    limit_per_host: int = Field(..., gt=0)
    ttl_dns_cache: int | None = Field(..., gt=0)


class RequestConfig(BaseModel):
    """Конфигурация HTTP-запросов.

    :ivar max_concurrent_requests: максимальное количество одновременных запросов
    :vartype max_concurrent_requests: int
    :ivar retry_attempts: количество попыток повторного запроса
    :vartype retry_attempts: int
    :ivar min_delay: минимальная задержка между запросами
    :vartype min_delay: float
    :ivar max_delay: максимальная задержка между запросами
    :vartype max_delay: float
    :ivar batch_size: размер пакета запросов
    :vartype batch_size: int
    :ivar max_workers: максимальное количество рабочих процессов
    :vartype max_workers: int
    :ivar buffer_size: размер буфера
    :vartype buffer_size: int
    :ivar timeout: таймаут запроса
    :vartype timeout: int
    :ivar session: настройки сессии
    :vartype session: SessionConfig
    """

    max_concurrent_requests: int = Field(..., gt=0)
    retry_attempts: int = Field(..., ge=0)
    min_delay: float = Field(..., gt=0)
    max_delay: float = Field(..., gt=0)
    batch_size: int = Field(..., gt=0)
    max_workers: int = Field(..., gt=0)
    buffer_size: int = Field(..., gt=0)
    timeout: int = Field(..., gt=0)
    session: SessionConfig


class ParserConfig(BaseModel):
    """Основная конфигурация парсера.

    :ivar pages: настройки страниц
    :vartype pages: PagesConfig
    :ivar save: настройки сохранения
    :vartype save: SaveConfig
    :ivar request: настройки запросов
    :vartype request: RequestConfig
    :ivar headers: настройки заголовков
    :vartype headers: HeadersConfig | None
    :ivar logging: настройки логирования
    :vartype logging: LoggingConfig | None
    """

    pages: PagesConfig
    save: SaveConfig
    request: RequestConfig
    headers: HeadersConfig | None = None
    logging: LoggingConfig | None = None

    @model_validator(mode="after")
    def check_pages(self) -> ParserConfig:
        """Проверяет корректность настройки страниц.

        :return: проверенная конфигурация
        :rtype: ParserConfig
        :raises ValueError: если pages является строкой, но не 'all'
        """
        if isinstance(self.pages, str) and self.pages != "all":
            error_message = "If type of pages is str, it must be 'all'"
            raise ValueError(error_message)
        return self
