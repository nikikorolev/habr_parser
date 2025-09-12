"""Модуль для экспорта данных в различные форматы."""

import csv
import json
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


class PExporter(Protocol):
    """Протокол для классов для экспорта данных.

    Определяет интерфейс для классов, осуществляющих экспорт данных.
    """

    def __init__(self, path: Path) -> None:
        """Инициализация параметров."""
        ...

    def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        ...

    def finalize(self) -> None:
        """Завершает процесс экспорта и закрывает ресурсы."""
        ...


class JsonExporter(PExporter):
    """Экспортер данных в формате JSON.

    :ivar path: путь к файлу для сохранения
    :vartype path: Path
    :ivar file: файловый объект для записи
    :vartype file: TextIO
    :ivar first_item: флаг первого элемента в массиве JSON
    :vartype first_item: bool
    """

    def __init__(self, path: Path) -> None:
        """Инициализация параметров."""
        self.path: Path = path.with_suffix(".json")
        self.file = self.path.open("w", encoding="utf-8")
        self.file.write("[\n")
        self.first_item: bool = True

    def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных в формате JSON.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        if not self.first_item:
            self.file.write(",\n")
        else:
            self.first_item = False
        json.dump(data, self.file, ensure_ascii=False)
        self.first_item = False

    def finalize(self) -> None:
        """Завершает запись JSON файла и закрывает его."""
        self.file.write("\n]")
        self.file.close()


class CsvExporter(PExporter):
    """Экспортер данных в формате CSV.

    :ivar path: путь к файлу для сохранения
    :vartype path: Path
    :ivar file: файловый объект для записи
    :vartype file: TextIO
    :ivar writer: объект для записи CSV данных
    :vartype writer: csv.DictWriter | None
    :ivar fieldnames: множество имен полей CSV
    :vartype fieldnames: set[str]
    """

    def __init__(self, path: Path) -> None:
        """Инициализация параметров."""
        self.path: Path = path.with_suffix(".csv")
        self.file = self.path.open("w", encoding="utf-8", newline="")
        self.writer: csv.DictWriter | None = None
        self.fieldnames: set[str] = set()

    def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных в формате CSV.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        self.fieldnames = set(data.keys())
        if self.writer is None:
            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
            self.writer.writeheader()

        self.writer.writerows([data])

    def finalize(self) -> None:
        """Завершает запись CSV файла и закрывает его."""
        self.file.close()


class ParquetExporter(PExporter):
    """Экспортер данных в формате Parquet.

    :ivar path: путь к файлу для сохранения
    :vartype path: Path
    :ivar data_chunks: список порций данных для объединения
    :vartype data_chunks: list[pd.DataFrame]
    """

    def __init__(self, path: Path) -> None:
        """Инициализация параметров."""
        self.path: Path = path.with_suffix(".parquet")
        self.data_chunks: list[pd.DataFrame] = []

    def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных для последующего экспорта в Parquet.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        if data:
            self.data_chunks.append(pd.DataFrame(data))

    def finalize(self) -> None:
        """Сохраняет все данные в Parquet файл."""
        if self.data_chunks:
            pd.concat(self.data_chunks).to_parquet(self.path, engine="pyarrow")


class Exporter:
    """Фасад для работы с экспортерами различных форматов.

    :ivar path: путь к файлу для сохранения
    :vartype path: Path
    :ivar extension: расширение файла определяющее формат экспорта
    :vartype extension: str
    :ivar exporter: экземпляр экспортера
    :vartype exporter: PExporter | None
    """

    def __init__(self, path: Path) -> None:
        """Инициализация параметров."""
        self.path: Path = path
        self.extension: str = path.suffix
        self.exporter: PExporter | None = None

    def __get_exporter(self) -> PExporter:
        """Создает и возвращает экспортер в зависимости от расширения файла.

        :return: экземпляр экспортера
        :rtype: PExporter
        :raises ValueError: если формат не поддерживается
        """
        if self.exporter is None:
            match self.extension:
                case ".json":
                    self.exporter = JsonExporter(self.path)
                case ".csv":
                    self.exporter = CsvExporter(self.path)
                case ".parquet":
                    self.exporter = ParquetExporter(self.path)
                case _:
                    error_message = f"Unsupported format: {self.extension}"
                    raise ValueError(error_message)
        return self.exporter

    def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных с использованием соответствующего экспортера.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        self.__get_exporter().save_chunk(data)

    def finalize(self) -> None:
        """Завершает процесс экспорта данных."""
        if (exporter := self.exporter):
            exporter.finalize()
