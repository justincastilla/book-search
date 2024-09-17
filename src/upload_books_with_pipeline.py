import os
import logging
import json
from dotenv import load_dotenv

from elastic_client import es
from elasticsearch import helpers

from sentence_transformers import SentenceTransformer


load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

INDEX_NAME = f"{os.environ.get('INDEX_NAME')}-cloud"
MODEL_ID = os.environ.get("MODEL_ID")

def create_ingest_pipeline():
    resp = es.ingest.put_pipeline(
        id="text-embedding",
        description="converts book description text to a vector",
        processors=[
            {
                "inference": {
                    "model_id": "sentence-transformers__msmarco-minilm-l-12-v3",
                    "input_output": [
                        {
                            "input_field": "book_description",
                            "output_field": "description_embedding",
                        }
                    ],
                }
            }
        ],
        on_failure=[
            {
                "set": {
                    "description": "Index document to 'failed-<index>'",
                    "field": "_index",
                    "value": "failed-{{{_index}}}",
                }
            },
            {
                "set": {
                    "description": "Set error message",
                    "field": "ingest.failure",
                    "value": "{{_ingest.on_failure_message}}",
                }
            },
        ],
    )
    logger.info(f"Created ingest pipeline: {resp}")


def create_books_index():
    mappings = {
        "mappings": {
            "properties": {
                "book_title": {"type": "text"},
                "author_name": {"type": "text"},
                "rating_score": {"type": "float"},
                "rating_votes": {"type": "integer"},
                "review_number": {"type": "integer"},
                "book_description": {"type": "text"},
                "genres": {"type": "keyword"},
                "year_published": {"type": "integer"},
                "url": {"type": "text"},
            }
        }
    }

    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=mappings)
        logger.info(f"Index '{INDEX_NAME}' created.")
    else:
        es.indices.delete(index=INDEX_NAME)
        logger.info(
            f"Index '{INDEX_NAME}' already exists. Deleting and recreating the index."
        )
        es.indices.delete(index=f"failed-{INDEX_NAME}", ignore_unavailable=True)
        logger.info(f"Index 'failed-{INDEX_NAME}' deleted.")

        es.indices.create(index=INDEX_NAME, body=mappings)
        logger.info(f"Index '{INDEX_NAME}' created.")

def create_one_book(book):

    try:
        resp = es.index(
            index=INDEX_NAME,
            id=book.get("id", None),
            body=book,
            pipeline="text-embedding",
        )

        logger.info(f"Successfully indexed book: {book.get('book_title', None)} - Result: {resp.get('result', None)}")

    except Exception as e:
        logger.error(f"Error occurred while indexing book: {e}")

def bulk_ingest_books(file_path="../data/books.json"):
    with open(file_path, "r") as file:
        books = json.load(file)

    actions = [
        {"_index": INDEX_NAME, "_id": book.get("id", None), "_source": book}
        for book in books
    ]

    try:
        helpers.bulk(es, actions, pipeline="text-embedding", chunk_size=1000)
        logger.info(f"Successfully ingested books into the '{INDEX_NAME}' index.")

    except helpers.BulkIndexError as e:
        logger.error(f"Error occurred while ingesting books: {e}")


create_ingest_pipeline()
create_books_index()
bulk_ingest_books()


with open("../data/fake_book.json", "r") as file:
    fake_book = json.load(file)
    create_one_book(fake_book[0])