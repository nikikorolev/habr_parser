"""Асинхронный модуль для экспорта данных в различные форматы (JSON, CSV, Parquet)."""

import asyncio
import csv
import json
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import aiofiles
import pandas as pd

if TYPE_CHECKING:
    from aiofiles.base import AiofilesContextManager

class PExporter(Protocol):
    """Протокол для классов для экспорта данных.

    Определяет интерфейс для классов, осуществляющих экспорт данных.
    """

    async def save_chunk(self, data: dict[str, Any]) -> None:
        """Асинхронно сохраняет порцию данных.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """

    async def finalize(self) -> None:
        """Асинхронно завершает процесс экспорта и закрывает ресурсы."""


class Exporter:
    """Фабрика для экспорта данных в различные форматы.

    :param path: путь к файлу для экспорта
    :type path: Path
    :param buffer_size: размер буфера для записи данных, defaults to 100
    :type buffer_size: int
    """

    def __init__(self, path: Path, buffer_size: int = 100, max_workers: int = 100) -> None:
        """Инициализация параметров."""
        self.path: Path = path
        self.extension: str = path.suffix.lower()
        self.buffer_size = buffer_size
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.__exporter: PExporter | None = None

    def __get_exporter(self) -> PExporter:
        """Получает экземпляр экспортера для соответствующего формата.

        :return: экземпляр экспортера
        :rtype: PExporter
        :raises ValueError: если формат не поддерживается
        """
        if self.__exporter is None:
            if self.extension == ".json":
                self.__exporter = JsonExporter(self.path, self.buffer_size, self.executor)
            elif self.extension == ".csv":
                self.__exporter = CsvExporter(self.path, self.buffer_size, self.executor)
            elif self.extension == ".parquet":
                self.__exporter = ParquetExporter(self.path, self.buffer_size, self.executor)
            else:
                error_msg = f"Unsupported format: {self.extension}"
                raise ValueError(error_msg)
        return self.__exporter

    async def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных через соответствующий экспортер.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        await self.__get_exporter().save_chunk(data)

    async def finalize(self) -> None:
        """Завершает процесс экспорта и освобождает ресурсы."""
        if self.__exporter:
            await self.__get_exporter().finalize()
        self.executor.shutdown()


class JsonExporter(PExporter):
    """Экспортер данных в формате JSON.

    :param path: путь к файлу для экспорта
    :type path: Path
    :param buffer_size: размер буфера для записи данных
    :type buffer_size: int
    :param executor: исполнитель для потоковых операций
    :type executor: ThreadPoolExecutor
    """

    def __init__(self, path: Path, buffer_size: int, executor: ThreadPoolExecutor) -> None:
        """Инициализация параметров."""
        self.path = path.with_suffix(".json")
        self.buffer_size: int = buffer_size
        self.executor: ThreadPoolExecutor = executor
        self.buffer: list[str] = []
        self.first_item: bool = True
        self.file: AiofilesContextManager | None = None

    async def __initialize_file(self) -> None:
        """Инициализирует файл для записи JSON данных."""
        if self.file is None:
            self.file = await aiofiles.open(self.path, "w", encoding="utf-8")
            await self.file.write("[\n")

    async def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных в формате JSON.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        await self.__initialize_file()

        loop = asyncio.get_running_loop()
        json_str = await loop.run_in_executor(
            self.executor,
            lambda: json.dumps(data, ensure_ascii=False),
        )

        if not self.first_item:
            json_str = ",\n" + json_str
        else:
            self.first_item = False

        self.buffer.append(json_str)

        if len(self.buffer) >= self.buffer_size:
            await self.__flush_buffer()

    async def __flush_buffer(self) -> None:
        """Сбрасывает буфер данных в файл."""
        if self.buffer and self.file:
            content = "".join(self.buffer)
            await self.file.write(content)
            self.buffer.clear()

    async def finalize(self) -> None:
        """Завершает запись JSON файла и закрывает ресурсы."""
        await self.__flush_buffer()
        if self.file:
            await self.file.write("\n]")
            await self.file.close()


class CsvExporter(PExporter):
    """Экспортер данных в формате CSV.

    :param path: путь к файлу для экспорта
    :type path: Path
    :param buffer_size: размер буфера для записи данных
    :type buffer_size: int
    :param executor: исполнитель для потоковых операций
    :type executor: ThreadPoolExecutor
    """

    def __init__(self, path: Path, buffer_size: int, executor: ThreadPoolExecutor) -> None:
        """Инициализация параметров."""
        self.path = path.with_suffix(".csv")
        self.buffer_size = buffer_size
        self.executor = executor
        self.fieldnames: set[str] = set()
        self.buffer: list[str] = []
        self.header_written = False
        self.file = None

    async def _initialize_file(self) -> None:
        """Инициализирует файл для записи CSV данных."""
        if self.file is None:
            self.file = await aiofiles.open(self.path, "w", encoding="utf-8", newline="")

    async def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных в формате CSV.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        await self._initialize_file()
        self.fieldnames.update(data.keys())

        loop = asyncio.get_running_loop()
        csv_line = await loop.run_in_executor(
            self.executor,
            self.__convert_to_csv_line,
            data,
        )

        self.buffer.append(csv_line)

        if not self.header_written:
            await self._write_header()
            self.header_written = True

        if len(self.buffer) >= self.buffer_size:
            await self.__flush_buffer()

    def __convert_to_csv_line(self, data: dict[str, Any]) -> str:
        """Конвертирует данные в CSV строку без использования DictWriter с файлом.

        :param data: данные для конвертации
        :type data: dict[str, Any]
        :return: CSV строка
        :rtype: str
        """
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=self.fieldnames)
        writer.writerow(data)
        csv_string = output.getvalue().strip()
        output.close()

        return csv_string + "\n"

    async def _write_header(self) -> None:
        """Записывает заголовок CSV файла."""
        if self.file:
            loop = asyncio.get_running_loop()
            header = await loop.run_in_executor(
                self.executor,
                lambda: ",".join(self.fieldnames) + "\n",
            )
            await self.file.write(header)

    async def __flush_buffer(self) -> None:
        """Сбрасывает буфер данных в файл."""
        if self.buffer and self.file:
            content = "".join(self.buffer)
            await self.file.write(content)
            self.buffer.clear()

    async def finalize(self) -> None:
        """Завершает запись CSV файла и закрывает ресурсы."""
        await self.__flush_buffer()
        if self.file:
            await self.file.close()


class ParquetExporter(PExporter):
    """Экспортер данных в формате Parquet.

    :param path: путь к файлу для экспорта
    :type path: Path
    :param buffer_size: размер буфера для записи данных
    :type buffer_size: int
    :param executor: исполнитель для потоковых операций
    :type executor: ThreadPoolExecutor
    """

    def __init__(self, path: Path, buffer_size: int, executor: ThreadPoolExecutor) -> None:
        """Инициализация параметров."""
        self.path = path.with_suffix(".parquet")
        self.buffer_size = buffer_size
        self.executor = executor
        self.data_chunks: list[dict[str, Any]] = []

    async def save_chunk(self, data: dict[str, Any]) -> None:
        """Сохраняет порцию данных в формате Parquet.

        :param data: данные для сохранения
        :type data: dict[str, Any]
        """
        self.data_chunks.append(data)

        if len(self.data_chunks) >= self.buffer_size:
            await self.__save_chunks()

    async def __save_chunks(self) -> None:
        """Сохраняет накопленные данные в Parquet файл."""
        if not self.data_chunks:
            return
        chunks_to_save = self.data_chunks.copy()
        self.data_chunks.clear()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            self.__save_to_parquet,
            chunks_to_save,
        )

    def __save_to_parquet(self, data_chunks: list[dict[str, Any]]) -> None:
        """Сохраняет данные в Parquet файл.

        :param data_chunks: список данных для сохранения
        :type data_chunks: list[dict[str, Any]]
        """
        if data_chunks:
            df = pd.DataFrame(data_chunks)
            df.to_parquet(self.path, engine="pyarrow", compression="snappy")

    async def finalize(self) -> None:
        """Завершает запись Parquet файла."""
        await self.__save_chunks()
