import uuid
from dataclasses import dataclass, fields
from datetime import datetime
from typing import List, Literal

from dateutil.parser import parse


@dataclass
class MixinId:
    id: uuid.UUID


@dataclass
class MixinTimeStamped:
    created: datetime
    modified: datetime


@dataclass
class Filmwork(MixinId, MixinTimeStamped):
    """Датакласс фильмов."""
    title: str
    description: str
    type: Literal["movie", "tv_show"]
    genres: List[str]
    persons: List[dict]
    rating: float

    def _append(self, list_attr: str, obj) -> None:
        if not hasattr(self, list_attr):
            setattr(self, list_attr, [])
        getattr(self, list_attr).append(obj)

    def __post_init__(self) -> None:
        """После инициализации добавить необходимые поля документа
        для Elasticsearch.
        """
        for own_field in fields(type(self)):
            if isinstance(own_field.type, datetime):
                value = getattr(self, own_field.name)
                if isinstance(value, str):
                    setattr(self, own_field.name, parse(value))

        role_to_names_map = {
            'director': 'director',
            'actor': 'actors_names',
            'writer': 'writers_names',
        }
        role_to_objs_map = {
            'actor': 'actors',
            'writer': 'writers',
        }
        for person in self.persons:
            role = person['role']
            name_attr = role_to_names_map[role]
            self._append(name_attr, person['name'])
            objs_attr = role_to_objs_map.get(role)
            if objs_attr:
                obj = {
                    'id': person['id'],
                    'name': person['name'],
                }
                self._append(objs_attr, obj)

    def as_document(self) -> dict:
        """
        Метод представления объекта в виде словаря.
        :return:
        """
        doc_mapping = {
            'id': 'id',
            'imdb_rating': 'rating',
            'genre': 'genre',
            'title': 'title',
            'description': 'description',
            'director': 'director',
            'actors_names': 'actors_names',
            'writers_names': 'writers_names',
            'actors': 'actors',
            'writers': 'writers',
        }
        doc = {}
        for doc_attr, obj_attr in doc_mapping.items():
            doc[doc_attr] = getattr(self, obj_attr, [])
        return doc
