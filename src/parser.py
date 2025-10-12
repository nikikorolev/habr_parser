"""Модуль для парсинга статей с сайта Habr.com."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
import yaml
from bs4 import BeautifulSoup

from src.models.config import ParserConfig
from src.utils.exceptions import FetchPostError
from src.utils.export import Exporter
from src.utils.logger import setup_logger

if TYPE_CHECKING:
    import types

    from src.models.config import HeadersConfig

HTTP_OK: int = 200
HTTP_530: int = 530
HTTP_TOO_MANY_REQUESTS: int = 429


class HabrParser:
    """Парсер статей с сайта Habr.com.

    :cvar BASE_URL: Базовый URL для статей Habr
    :vartype BASE_URL: str
    :cvar BASE_CONFIG_PATH: Путь к файлу конфигурации по умолчанию
    :vartype BASE_CONFIG_PATH: Path
    """

    BASE_URL: str = "https://habr.com/ru/articles/"
    BASE_CONFIG_PATH: Path = Path(__file__).parent.parent.joinpath("config.yaml")

    def __init__(self, config_path: str | Path | None = None) -> None:
        """Инициализация параметров.

        :param config_path: Путь к файлу конфигурации
        :type config_path: str | Path | None
        """
        path = Path(config_path) if config_path else self.BASE_CONFIG_PATH
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        self.config: ParserConfig = ParserConfig(**data)
        setup_logger(self.config.logging)
        self.log: logging.Logger = logging.getLogger(__name__)
        self.session: aiohttp.ClientSession | None = None
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(self.config.request.max_concurrent_requests)
        self.last_request_time: float = 0
        self.request_count: int = 0
        self.log.info("HabrParser initialized")

    async def __aenter__(self) -> HabrParser:
        """Асинхронный контекстный менеджер для инициализации сессии.

        :return: Экземпляр парсера
        :rtype: HabrParser
        """
        await self.init_session()
        return self

    async def __aexit__(self,
                    exc_type: type[BaseException] | None,
                    exc_val: BaseException | None,
                    exc_tb: types.TracebackType | None,
                   ) -> bool | None:
        """Асинхронный контекстный менеджер для закрытия сессии.

        :param exc_type: Тип исключения
        :type exc_type: type[BaseException] | None
        :param exc_val: Значение исключения
        :type exc_val: BaseException | None
        :param exc_tb: Трассировка исключения
        :type exc_tb: types.TracebackType | None
        :return: Флаг обработки исключения
        :rtype: bool | None
        """
        await self.close_session()
        return None

    async def init_session(self) -> None:
        """Инициализирует HTTP-сессию с настройками соединения."""
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.config.request.session.limit,
                limit_per_host=self.config.request.session.limit_per_host,
                ttl_dns_cache=self.config.request.session.ttl_dns_cache,
                force_close=self.config.request.session.force_close,
            )
            self.session = aiohttp.ClientSession(
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=self.config.request.timeout),
                connector=connector,
            )
            self.log.debug("Session initialized with rate limiting")

    async def close_session(self) -> None:
        """Закрывает HTTP-сессию."""
        if self.session and not self.session.closed:
            await self.session.close()

    @property
    def save_path(self) -> Path:
        """Возвращает путь для сохранения данных.

        :return: Путь для сохранения
        :rtype: Path
        """
        return self.config.save.get_path()

    @property
    def headers(self) -> dict[str, str | None]:
        """Возвращает заголовки HTTP-запросов.

        :return: Словарь заголовков
        :rtype: dict[str, str | None]
        """
        headers_config: HeadersConfig | None = self.config.headers
        return headers_config.build_headers() if headers_config else {}

    async def __delay_request(self) -> None:
        """Добавляет случайную задержку между запросами."""
        await asyncio.sleep(random.uniform(self.config.request.min_delay, self.config.request.max_delay))  # noqa: S311

    async def fetch_post(self, post_id: int) -> str:
        """Получает HTML-контент с retry и задержками.

        :param post_id: ID статьи для получения
        :type post_id: int
        :return: HTML-контент статьи
        :rtype: str
        :raises FetchPostError: Если не удалось получить статью после всех попыток
        """
        async with self.semaphore:
            await self.__delay_request()

            url = f"{self.BASE_URL}{post_id}"
            self.log.info(f"Fetching post {post_id}")

            for attempt in range(self.config.request.retry_attempts):
                try:
                    async with self.session.get(url) as response:
                        if response.status == HTTP_OK:
                            text = await response.text()
                            self.log.info(f"Successfully fetched post {post_id}")
                            return text
                        if response.status == HTTP_530:
                            self.log.warning(f"503 error for post {post_id}, attempt {attempt + 1}")
                            await self.__delay_request()
                            continue
                        if response.status == HTTP_TOO_MANY_REQUESTS:
                            self.log.warning(f"429 Rate Limited for post {post_id}")
                            await self.__delay_request()
                            continue
                        error_message = f"HTTP {response.status}"
                        raise FetchPostError(post_id, error_message)

                except aiohttp.ClientError as e:
                    self.log.warning(f"Client error, retrying: {e!s}")
                    await self.__delay_request()

            error_message = "Max retries exceeded"
            raise FetchPostError(post_id, error_message)

    def parse_post(self, text: str) -> dict[str, Any]:
        """Парсит HTML-контент статьи.

        :param text: HTML-контент статьи
        :type text: str
        :return: Словарь с распарсенными данными статьи
        :rtype: dict[str, Any]
        """
        soup = BeautifulSoup(text, "lxml")
        data: dict[str, Any] = {}

        if not soup.find("div", {"id": "post-content-body"}):
            data["status"] = "not_found"
        else:
            data["status"] = "ok"
            title = soup.find("title")
            data["title"] = title.get_text(strip=True) if title else None

            article = soup.find("div", {"class": "article-formatted-body"})
            data["text"] = article.get_text(strip=True) if article else None

            keywords = soup.find("meta", attrs={"name": "keywords"})
            keywords_content = keywords.get("content") if keywords else None
            data["keywords"] = keywords_content.split(", ") if keywords_content else None

            username = soup.find("a", {"class": "tm-user-info__username"})
            data["username"] = username.get_text(strip=True) if username else None

            hubs = []
            for link in soup.find_all("a", class_="tm-hubs-list__link"):
                hub_name = link.get_text(strip=True) if link else None
                hubs.append(hub_name)
            data["hubs"] = hubs

            time = soup.find("time")
            data["time"] = datetime.fromisoformat(
                time.get("datetime").replace("Z", "+00:00"),
                ).strftime("%Y-%m-%d %H:%M:%S") if time else None

            reading_time = soup.find("span", {"class": "tm-article-reading-time__label"})
            data["reading_time"] = reading_time.get_text(strip=True)[:-4] if reading_time else None

        return data

    async def get_post_data(self, post_id: int) -> dict[str, Any]:
        """Обрабатывает статью с обработкой ошибок.

        :param post_id: ID статьи для обработки
        :type post_id: int
        :return: Словарь с данными статьи или информацией об ошибке
        :rtype: dict[str, Any]
        """
        try:
            text = await self.fetch_post(post_id)
            data = self.parse_post(text)
        except FetchPostError as e:
            self.log.error(f"Failed to fetch post {post_id}: {e}")  # noqa: TRY400
            return {"id": post_id, "status": "fetch_error", "error": str(e)}
        except Exception as e:
            self.log.error(f"Unexpected error for post {post_id}: {e}")  # noqa: TRY400
            return {"id": post_id, "status": "error", "error": str(e)}
        else:
            data["id"] = post_id
            return data

    async def ingest_all(self) -> None:
        """Основной метод парсинга с улучшенным управлением скоростью."""
        pages = self.config.pages
        first, last = pages.first, pages.last
        self.log.info(f"Starting parsing from {first} to {last}!")
        exporter = Exporter(
            self.save_path,
            max_workers=self.config.request.max_workers,
            buffer_size=self.config.request.buffer_size,
            )

        try:
            await self.init_session()
            batch_size = self.config.request.batch_size
            all_post_ids = list(range(first, last + 1))

            for i in range(0, len(all_post_ids), batch_size):
                batch_ids = all_post_ids[i:i + batch_size]

                tasks = [self.get_post_data(pid) for pid in batch_ids]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, BaseException):
                        continue
                    if not self.config.save.skip or result.get("status") == "ok":
                        await exporter.save_chunk(result)

                self.log.info(f"Processed {min(i + batch_size, len(all_post_ids))}/{len(all_post_ids)}")

                if i + batch_size < len(all_post_ids):
                    await self.__delay_request()
        finally:
            await self.close_session()
            await exporter.finalize()

        self.log.info("Parsing completed")
