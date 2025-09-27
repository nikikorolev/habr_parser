"""CLI утилиты для парсера Habr.com."""

import asyncio
from pathlib import Path

import click

from src.parser import HabrParser


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=HabrParser.BASE_CONFIG_PATH,
    help="Путь к YAML файлу конфигурации",
)
def cli(config: Path) -> None:
    """Запускает парсинг статей с habr."""
    try:
        async def main() -> None:
            """Корутина для париснга."""
            async with HabrParser(config) as parser:
                await parser.ingest_all()
        asyncio.run(main())
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort from e


if __name__ == "__main__":
    cli()
