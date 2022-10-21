import logging
import os

from pydantic import BaseSettings, Field

logging.basicConfig(
    level=logging.INFO,
    filename='program.log',
    format='%(asctime)s, %(levelname)s, %(message)s, %(name)s'
)


class PostgresDSL(BaseSettings):
    dbname: str = Field(default='movies_database', env='DB_NAME')
    user: str = Field(default='app', env='DB_USER')
    password: str = Field(default='123qwe', env='DB_PASSWORD')
    host: str = Field(default='127.0.0.1', env='DB_HOST')
    port: str = Field(default='5432', env='DB_PORT')

    class Config:
        env_file = os.environ.get('PATH', default='../.env')
        env_file_encoding = 'utf-8'


class ElasticDSL(BaseSettings):
    host: str = Field(default='127.0.0.1', env='ELASTIC_HOST')
    port: str = Field(default='9200', env='ELASTIC_PORT')
    ES_INDEX_NAME: str = 'movies'
    ES_MAPPING: dict = {
        'settings': {
            'refresh_interval': '1s',
            'analysis': {
                'filter': {
                    'english_stop': {
                        'type': 'stop',
                        'stopwords': '_english_'
                    },
                    'english_stemmer': {
                        'type': 'stemmer',
                        'language': 'english'
                    },
                    'english_possessive_stemmer': {
                        'type': 'stemmer',
                        'language': 'possessive_english'
                    },
                    'russian_stop': {
                        'type': 'stop',
                        'stopwords': '_russian_'
                    },
                    'russian_stemmer': {
                        'type': 'stemmer',
                        'language': 'russian'
                    }
                },
                'analyzer': {
                    'ru_en': {
                        'tokenizer': 'standard',
                        'filter': [
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'english_possessive_stemmer',
                            'russian_stop',
                            'russian_stemmer'
                        ]
                    }
                }
            }
        },
        'mappings': {
            'dynamic': 'strict',
            'properties': {
                'id': {
                    'type': 'keyword'
                },
                'imdb_rating': {
                    'type': 'float'
                },
                'genre': {
                    'type': 'keyword'
                },
                'title': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'description': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'director': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'actors_names': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'writers_names': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'actors': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword'
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en'
                        }
                    }
                },
                'writers': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword'
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en'
                        }
                    }
                }
            },
        },
    }

    class Config:
        env_file = os.environ.get('PATH', default='../.env')
        env_file_encoding = 'utf-8'


class Settings(BaseSettings):
    FIRST_DATE: str = '2000-01-01'
    ETL_DELAY: int = Field(default=60, env='ETL_DELAY')
    BATCH_SIZE: int = 100
    STATE_FILE: str = Field(default='./state.json', env='FILEPATH_JSON')

    POSTGRES_DSL: PostgresDSL = PostgresDSL()
    ELASTIC_DSL: ElasticDSL = ElasticDSL()

    class Config:
        env_file = os.environ.get('PATH', default='../.env')
        env_file_encoding = 'utf-8'


SETTINGS = Settings()
