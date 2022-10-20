import inspect
import os
import sys
from contextlib import contextmanager
from typing import Generator, Tuple

import psycopg2
from psycopg2.extras import DictCursor, RealDictRow

currentdir = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from config import BATCH_SIZE, FIRST_DATE
from utils.storage import State
from utils.transformer import Filmwork


@contextmanager
def postgres_conn_context(
        dsl: dict
) -> Generator[psycopg2.connect, None, None]:
    """
    Открыть соединение с базой данных.
    :param dsl: параметры подключения
    :return: соединение
    """
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    yield conn
    conn.close()


@contextmanager
def postgres_cursor_context(
        conn: psycopg2.connect
) -> Generator[psycopg2.connect, None, None]:
    """
    Создание курсора
    :param conn:
    :return:
    """
    cursor = conn.cursor()
    yield cursor
    cursor.close()


class StateKeys:
    """Ключи словаря хранилища актуализации."""

    FILMWORK = 'movie_filmwork_md'
    PERSON = 'movie_person_md'
    GENRE = 'movie_genre_md'


class PostgresMovieExtractor:
    """Класс, выгружающий фильмы из PostgreSQL."""

    def __init__(self, conn: psycopg2.connect, state: State):
        """
        Инициализация параметров класса.
        :param conn: соединение с базой
        :param state: объект-хранилище
        """
        self.conn = conn
        self.state = state
        self.dataklass = Filmwork

    def get_tables(self) -> list:
        """Метод для получения всех названий таблиц в базе."""

        with postgres_cursor_context(self.conn) as cur:
            query = "SELECT name FROM sqlite_master WHERE type='table'"
            cur.execute(query)
            tables = [row["name"] for row in cur.fetchall()]
        return tables

    def _execute_raw(
            self,
            query: str,
            values: tuple,
            batch_size: int = BATCH_SIZE
    ) -> Generator[list, None, None]:
        """
        Метод для получения данных из базы по запросу.
        :param query: запрос
        :param values: параметры для запроса
        :param batch_size: размер батча
        :return:
        """
        with postgres_cursor_context(self.conn) as cur:
            cur.execute(query, values)
            while rows := cur.fetchmany(batch_size):
                yield rows

    @staticmethod
    def _split_batch(batch) -> Generator[Tuple[Tuple[str], str], None, None]:
        """
        Извлечб ID и раннюю дату модификации из батча данных.
        :param batch: батч данных
        :return:
        """
        ids, modified_dates = zip(*(row.values() for row in batch))
        since = modified_dates[0]
        yield ids, since

    def ids_film_work_since_date(
            self,
            since: str = FIRST_DATE
    ) -> Generator[Tuple[Tuple[str], str], None, None]:
        """
        Получить ID фильмов, отредактированных с указанной даты.
        :param since: дата модификации
        :return:
        """
        query = """
            SELECT
                fw.id,
                fw.modified
            FROM film_work fw
            WHERE fw.modified >= %s
            ORDER BY fw.modified, fw.id;
        """
        values = (since,)
        for batch in self._execute_raw(query, values):
            yield from self._split_batch(batch)

    def ids_genre_since_date(
            self, since: str = FIRST_DATE
    ) -> Generator[Tuple[Tuple[str], str], None, None]:
        """Получить ID фильмов, у которых изменился жанр.
        """
        query = """
           SELECT gfw.film_work_id,
                   g.modified
            FROM genre g
            INNER JOIN genre_film_work gfw ON g.id = gfw.genre_id
            WHERE g.modified >= %s
            ORDER BY g.modified, gfw.film_work_id;
        """
        values = (since,)
        for batch in self._execute_raw(query, values):
            yield from self._split_batch(batch)

    def ids_person_since_date(
            self,
            since: str = FIRST_DATE
    ) -> Generator[Tuple[Tuple[str], str], None, None]:
        """Получить ID фильмов, у которых изменились персоны."""
        query = """
            SELECT
                pfw.film_work_id,
                p.modified
            FROM person p
            INNER JOIN person_film_work pfw ON p.id = pfw.person_id
            WHERE p.modified >= %s
            ORDER BY p.modified, pfw.film_work_id;
        """
        values = (since,)
        for batch in self._execute_raw(query, values):
            yield from self._split_batch(batch)

    def get_filmworks(self,
                      ids: Tuple[str]) -> Generator[RealDictRow, None, None]:
        """
        Получить фильмы с указанными ID.
        :param ids: список id извлекаемых фильмов
        :return:
        """
        query = """
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                COALESCE (
                   json_agg(
                       DISTINCT jsonb_build_object(
                           'role', pfw.role,
                           'id', p.id,
                           'name', p.full_name
                       )
                   ) FILTER (WHERE p.id is not null),
                   '[]'
                ) as persons,
                array_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s
            GROUP BY fw.id
            ORDER BY fw.modified;
        """
        values = (tuple(ids),)
        rows = self._execute_raw(query, values)
        yield from rows

    def extract_all(self) -> Generator[RealDictRow, None, None]:
        """
        Извлечь все обновленные фильмы.
        :return:
        :rtype:
        """

        genre_since = self.state.get_state(StateKeys.GENRE) or FIRST_DATE
        flag = True
        for ids, genre_since in self.ids_genre_since_date(genre_since):
            if flag:
                self.state.set_state(StateKeys.GENRE, genre_since)
                flag = False
            yield from self.get_filmworks(ids)

        person_since = self.state.get_state(StateKeys.PERSON) or FIRST_DATE
        flag = True
        for ids, person_since in self.ids_person_since_date(person_since):
            if flag:
                self.state.set_state(StateKeys.PERSON, person_since)
                flag = False
            yield from self.get_filmworks(ids)

        film_work_since = self.state.get_state(
            StateKeys.FILMWORK) or FIRST_DATE
        flag = True
        for ids, fw_since in self.ids_film_work_since_date(film_work_since):
            if flag:
                self.state.set_state(StateKeys.FILMWORK, fw_since)
                flag = False
            yield from self.get_filmworks(ids)
