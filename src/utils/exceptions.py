"""Исключения для модуля парсера."""


class FetchPostError(Exception):
    """Исключение, возникающее при ошибке получения статьи.

    :ivar post_id: идентификатор статьи, которую не удалось получить
    :vartype post_id: int
    :ivar status_code: код статуса HTTP или описание ошибки
    :vartype status_code: str
    """

    def __init__(self, post_id: int, status_code: str) -> None:
        """Инициализация параметров."""
        error_message = f"Failed to fetch post {post_id}, status code {status_code}"
        super().__init__(error_message)
        self.post_id = post_id
        self.status_code = status_code
