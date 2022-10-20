# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL.
Подробности задания в папке `etl`.


## Установка и запуск проекта:

1. Клонировать репозиторий на локальную машину.
```https://github.com/Oorzhakau/new_admin_panel_sprint_3.git```
2. Создать файл .env на основе .env_example в корневом каталоге `docker-compose/`
и указать значение переменных окружения:
```
# === Database Postgres ===

PWD=/home/user (корневая директория, где расположе volume с тестовой базой $PWD/postgresql/data)
POSTGRES_DB_ENGINE=django.db.backends.postgresql
POSTGRES_DB=movies_database
POSTGRES_USER=user
POSTGRES_PASSWORD=pass
POSTGRES_DB_HOST=db
POSTGRES_DB_PORT=5432
POSTGRES_DB_SCHEMA=content

# === Elasticsearch ===
ELASTIC_HOST=elastic
ELASTIC_PORT=9200

# === MAIN ===
ETL_DELAY=60 (время ожидания фоновой задачи)
FILEPATH_JSON='./state.json' (путь до файла с хранением )
```
3. Выполнить в корневой директории команду:
```
docker-compose -f docker-compose.yml up --build
```
