import inspect
import os
import sys
from contextlib import contextmanager
from typing import Generator, List, Optional

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError

currentdir = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from config import BATCH_SIZE, ES_MAPPING, logging


@contextmanager
def es_create_connection(**kwargs):
    es = Elasticsearch(
        hosts=f"http://{kwargs['host']}:{kwargs['port']}/"
    )
    yield es
    es.transport.close()


class ElasticLoader:
    """Загрузчик фильмов в индекс Elasticsearch."""

    def __init__(self,
                 es_client: Elasticsearch,
                 index: str,
                 mapping: dict = ES_MAPPING,
                 batch_size: int = BATCH_SIZE):
        """
        Инициализация загрузчика
        :param es_client: соединение с Elasticsearch сервером
        :param index: название индекса
        :param mapping: маппинг индекса
        :param batch_size: размер батча
        """
        self.es_client = es_client
        self.index = index
        self.mapping = mapping
        self._documents = []
        self._batch_size = batch_size

        self._create_index()

    def _create_index(self) -> None:
        """
        Метод создания индекса.
        :return:
        """
        if not self.es_client.indices.exists(index=self.index):
            logging.info(f"Create index - {self.index}.")
            self.es_client.indices.create(index=self.index, body=self.mapping)
            return
        logging.info(f"Index {self.index} is already created.")

    def get(self, id: str) -> Optional[dict]:
        """
        Получить документ по id.
        :param id: id документа
        :return:
        """
        try:
            return self.es_client.get(index=self.index, id=id)
        except NotFoundError:
            return

    def add_in_batch(self, document: dict) -> None:
        """
        Добавить документ в текущий батч.
        :param document:
        """
        self._documents.append(document)

    def is_batch_ready(self) -> bool:
        """
        Проверить заполненность батча.
        :return:
        """
        return len(self._documents) >= self._batch_size

    def save(self) -> None:
        """
        Сохранить все объекты из буфера в ElasticSearch.
        :return:
        """

        def get_actions(documents: List[dict]) -> Generator[dict, None, None]:
            for document in documents:
                action = {
                    '_index': self.index,
                    '_id': document['id'],
                    '_source': document,
                }
                yield action

        helpers.bulk(self.es_client, get_actions(self._documents))
        self._documents = []
