# Парсер Habr
Пакет для парсинга статей с сайта habr.com. 
> **⚠️ ВАЖНО:** Модуль находится в активной стадии разработки, поэтому процитирую слова великих: **если что-то не нравится - контрибьють**

# Установка

Для начала скачаем актуальную версию проекта:

```bash
git clone https://github.com/nikikorolev/habr_parser.git
cd habr_parser
```

Синхронизируем зависимости с помощью `uv`:
```bash
uv sync
```
Или можно воспользоваться другим пакетным менеджером, но я рекоммендую делать все через `uv`. 

# Запуск парсинга

Внутри пакета уже доступна команда `habr-parser`, запуск через `uv` будет выглядеть следующим образом:

```bash
uv run habr-parser
```

Можно, например, запустить по-другому: активировав виртуальную среду, тогда запуск будет вообще в одну команду:
```bash
source .venv/bin/activate
habr-parser
```

Если вбить *habr-parser --help*, то можно получить вот такую справку:
```bash
Usage: habr-parser [OPTIONS]

  Запускает парсинг статей с Habr.com.

Options:
  -c, --config FILE  Путь к YAML файлу конфигурации
  --help             Show this message and exit.
```
в которой упоминается некий `YAML-файл`. Основная настройка парсера должна находится в нем. Чтобы не юзать ключ `-c` создайте файл `config.yaml` на уровне с `src`, тогда он автоматически подтянется в парсер.
```bash
touch config.yaml
```
Пример такого файла:

```yaml
pages: # С какой и по какую id страницы habr'a парсить
  first: 1
  last: 5
save: # Параметры сохранения
  file: "data" # имя файла
  path: "path/to/.data" # Сохранять лучше в .data, она в .gitignore
  extension: "csv" # csv, parquet, json
  skip: True # Сохранять ли страницы с ошибками (404, 403...), skip: true не сохраняет
headers: # Заголовки для запроса, необязательный
  user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
  accept_language: "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
  accept_encoding: "gzip, deflate, br"
  connection: "keep-alive"
  referer: "https://www.google.com/"
logging: # Логгер, необязательный
  level: "DEBUG" # Уровень, аналогичный уровню в logging пакете
  output: "both" # both - в файл и консоль, console - в консоль, file - в файл
  filename: "parser.log" # Имя файла для логов, создается в папке log/ автоматом
```