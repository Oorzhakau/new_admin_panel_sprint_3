"""Хранение состояния загрузки данных в поисковой системе Elasticsearch."""

import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    """Хранилище данных в json-файле."""

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as f:
            json.dump(obj=state, fp=f, indent=4, default=str)

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            return {}


class State:
    """Класс для хранения состояния при работе с данными.
    Предназначен для хранения актуального состояния данных.
    """

    def __init__(self, storage: BaseStorage):
        """Проинициализировать состояние и хранилище состояния."""
        self.storage = storage
        self._dict = {}

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self._dict[key] = value
        self.storage.save_state(self._dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        self._dict = self.storage.retrieve_state()
        return self._dict.get(key)
