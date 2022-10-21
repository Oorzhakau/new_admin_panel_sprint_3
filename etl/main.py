"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

import logging
import time

from elasticsearch.exceptions import ConnectionError
from psycopg2 import OperationalError

from config import SETTINGS
from utils.backoff import backoff
from utils.elastic_loader import ElasticLoader, es_create_connection
from utils.postgres_extractor import (PostgresMovieExtractor,
                                      postgres_conn_context)
from utils.storage import JsonFileStorage, State

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load(extractor: PostgresMovieExtractor, loader: ElasticLoader) -> None:
    """
    Загрузка батчей данных из базы данных Postgres в индекс Elasticsearch.

    :param extractor: объект, извлекающий из базы данных класс.
    :param loader: объект, загружающий документы в полнотекстовый индекс.
    :return:
    """
    dataklass = extractor.dataklass
    for batch in extractor.extract_all():
        for row in batch:
            obj = dataklass(**row)
            loader.add_in_batch(obj.as_document())
        if loader.is_batch_ready():
            loader.save()
    loader.save()


@backoff((ConnectionError, OperationalError))
def etl() -> None:
    """
    Функция, описывающая процесс ETL.
    :return:
    """
    logging.info('Initializing postgresql and elasticsearch connection.')
    with postgres_conn_context(SETTINGS.POSTGRES_DSL.dict()) as pg_conn, \
            es_create_connection(**SETTINGS.ELASTIC_DSL.dict()) as es_client:
        state = State(JsonFileStorage(SETTINGS.STATE_FILE))
        loader = ElasticLoader(es_client, SETTINGS.ELASTIC_DSL.ES_INDEX_NAME)
        ext_obj = PostgresMovieExtractor(pg_conn, state)
        logger.info('Started loading.')
        load(ext_obj, loader)
        logger.info('Finished loaded.')


if __name__ == '__main__':
    while True:
        etl()
        time.sleep(SETTINGS.ETL_DELAY)
