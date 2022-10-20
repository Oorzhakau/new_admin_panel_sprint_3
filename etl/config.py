import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    filename='program.log',
    format='%(asctime)s, %(levelname)s, %(message)s, %(name)s'
)

POSTGRES_DB = {
    'dbname': os.getenv('POSTGRES_DB', default='movies_database'),
    'user': os.getenv('POSTGRES_USER', default='app'),
    'password': os.getenv('POSTGRES_PASSWORD', default='123qwe'),
    'host': os.getenv('POSTGRES_DB_HOST', default='127.0.0.1'),
    'port': os.getenv('POSTGRES_DB_PORT', default='5432'),
}

ELASTIC_HOST = {
    'host': os.getenv('ELASTIC_HOST', default='127.0.0.1'),
    'port': os.getenv('ELASTIC_PORT', '9200'),
}

BATCH_SIZE = 100

ETL_DELAY = int(os.getenv('ETL_DELAY', default=60))

STATE_FILE = os.getenv('FILEPATH_JSON', default='./state.json')

ES_INDEX_NAME = 'movies'

ES_MAPPING = {
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

FIRST_DATE = '2000-01-01'
