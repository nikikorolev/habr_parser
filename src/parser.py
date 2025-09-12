"""Модуль для парсинга статей с сайта Habr.com."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests
import yaml
from bs4 import BeautifulSoup

from src.models.config import ParserConfig
from src.utils.exceptions import FetchPostError
from src.utils.export import Exporter
from src.utils.logger import setup_logger

if TYPE_CHECKING:
    from src.models.config import HeadersConfig


HTTP_OK: int = 200

# TODO: доработки после обсуждения
#   0. Асинхронность
#   1. Парсинг комментариев
#   2. Исправить алерты mypy


class HabrParser:
    """Парсер статей с сайта Habr.com.

    Чтобы начать пользоватся парсером, нужно составить конфиг с помощью `ParserConfig`,
    либо воспользоватся YAML-файликом. Для парсинга, воспользуйтесь методом `ingest`.


    :cvar BASE_URL: базовый URL для статей Habr
    :vartype BASE_URL: str
    :cvar BASE_CONFIG_PATH: путь к файлу конфигурации по умолчанию
    :vartype BASE_CONFIG_PATH: Path
    :cvar TIMEOUT: таймаут HTTP-запросов в секундах
    :vartype TIMEOUT: int

    :ivar config: конфигурация парсера
    :vartype config: ParserConfig
    :ivar log: логгер для записи событий
    :vartype log: logging.Logger
    """

    BASE_URL: str = "https://habr.com/ru/articles/"
    BASE_CONFIG_PATH: Path = Path(__file__).parent.parent.joinpath("config.yaml")
    TIMEOUT: int = 10

    def __init__(self, config_path: str | Path | None = None) -> None:
        """Инициализация параметров."""
        path = Path(config_path) if config_path else self.BASE_CONFIG_PATH
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        self.config = ParserConfig(**data)
        setup_logger(self.config.logging)
        self.log = logging.getLogger(__name__)
        init_message = "HabrParser initialized"
        self.log.info(init_message)

    @property
    def save_path(self) -> Path:
        """Путь для файла сохранения данных.

        :return: путь для сохранения файлов
        :rtype: Path
        """
        return self.config.save.get_path()

    @property
    def headers(self) -> dict[str, str | None]:
        """Формирует заголовки HTTP-запросов.

        :return: словарь с заголовками запросов
        :rtype: dict[str, str | None]
        """
        headers_config: HeadersConfig | None = self.config.headers
        return headers_config.build_headers() if headers_config else {}

    def fetch_post(self, post_id: int) -> str:
        """Получает HTML-контент статьи по идентификатору.

        :param post_id: идентификатор статьи
        :type post_id: int
        :return: HTML-контент статьи
        :rtype: str
        :raises FetchPostError: если произошла ошибка при получении статьи
        """
        debug_message = f"Fetching post {post_id}"
        self.log.debug(debug_message)
        url = f"{self.BASE_URL}{post_id}"
        try:
            response = requests.get(
                url,
                timeout=self.TIMEOUT,
                headers=self.headers,
            )
        except requests.RequestException as e:
            error_message = f"Request error for post {post_id}: {e}"
            self.log.exception(error_message)
            raise FetchPostError(post_id, str(e)) from e

        if response.status_code == HTTP_OK:
            success_message = f"Successfully fetched post {post_id}"
            self.log.debug(success_message)
            text: str = response.text
            return text
        warning_message = f"Failed to fetch post {post_id}: HTTP status {response.status_code}"
        self.log.warning(warning_message)
        raise FetchPostError(post_id, response.status_code)

    def parse_post(self, text: str) -> dict[str, Any]:
        """Парсит HTML-контент статьи и извлекает данные.

        :param text: HTML-контент статьи
        :type text: str
        :return: словарь с извлеченными данными статьи
        :rtype: dict[str, Any]
        """
        debug_message = "Parsing post content"
        self.log.debug(debug_message)
        soup = BeautifulSoup(text, "html5lib")
        data: dict[str, Any] = {}

        if not soup.find("div", {"id": "post-content-body"}):
            data["status"] = "not_found"
            not_found_message = "Post content not found"
            self.log.debug(not_found_message)
        else:
            data["status"] = "ok"
            content_found_message = "Post content found, extracting data"
            self.log.debug(content_found_message)

            title = soup.find("title")
            data["title"] = title.get_text(strip=True) if title else None
            if not data["title"]:
                title_warning = "Title not found in post"
                self.log.warning(title_warning)

            article = soup.find("div", {"class": "article-formatted-body"})
            data["text"] = article.get_text(strip=True) if article else None
            if not data["text"]:
                text_warning = "Article text not found"
                self.log.warning(text_warning)

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

            user_bio = soup.find("p", {"data-test-id": "user-card-speciality"})
            data["user_bio"] = user_bio.get_text(strip=True) if user_bio else None

        return data

    def get_post_data(self, post_id: int) -> dict[str, Any]:
        """Обрабатывает статью по идентификатору и возвращает данные в виде словаря.

        Словарь имеет вид следующие ключи:
            status - статус обработки страницы
            title - название статьи
            text - текст статьи
            keywords - ключевые слова
            username - имя автора статьи
            user_bio - описания профиля автора статьи
            hubs - хабы

        :param post_id: идентификатор статьи
        :type post_id: int
        :return: словарь с данными статьи
        :rtype: dict[str, Any]
        """
        info_message = f"Processing post {post_id}"
        self.log.info(info_message)
        data: dict[str, Any] = {}
        try:
            text = self.fetch_post(post_id)
            data = self.parse_post(text)
            status_message = f"Successfully processed post {post_id}, status: {data.get('status')}"
            self.log.info(status_message)
        except FetchPostError as e:
            data["status"] = "fetch_post_error"
            fetch_error_message = f"Failed to process post {post_id}, error: {e!s}"
            self.log.error(fetch_error_message)  # noqa: TRY400
        except Exception as e:
            data["status"] = "parse_error"
            parse_error_message = f"Unexpected error processing post {post_id}, error: {e!s}"
            self.log.error(parse_error_message)  # noqa: TRY400

        data["id"] = post_id
        return data

    def ingest_all(self) -> None:
        """Осуществляет парсинг всех статей в указанном диапазоне."""
        wrong_statuses = ["not_found", "fetch_post_error", "parse_error"]
        pages = self.config.pages
        first, last = pages.first, pages.last

        start_message = f"Starting parsing posts from {first} to {last}"
        self.log.info(start_message)
        exporter = Exporter(self.save_path)

        for post_id in range(first, last + 1):
            if post_id % 100 == 0:
                progress_message = f"Process {post_id} posts out of {last - first + 1}"
                self.log.info(progress_message)
            post_data = self.get_post_data(post_id)
            if self.config.save.skip and post_data.get("status") in wrong_statuses:
                skip_message = f"Skipping post {post_id} due to status: {post_data.get('status')}"
                self.log.debug(skip_message)
                continue
            try:
                exporter.save_chunk(post_data)
            except Exception:
                save_error_message = "Saving error"
                self.log.exception(save_error_message)
                continue
            success_message = f"Successfully saved post with {post_id}."
            self.log.info(success_message)

        exporter.finalize()
        finish_message = f"Finished parsing {last - first + 1} posts"
        self.log.info(finish_message)


if __name__ == "__main__":
    parser = HabrParser()
    parser.ingest_all()
