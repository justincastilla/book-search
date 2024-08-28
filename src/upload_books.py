import os
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import logging 
import json
from elasticsearch.helpers import BulkIndexError


load_dotenv(override=True)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


CLOUD_ID = os.environ.get('CLOUD_ID')
print(CLOUD_ID)
ELASTIC_USERNAME = os.environ.get('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')
INDEX_NAME = "books_parallel"

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    request_timeout=60,  # Increase the timeout to 60 seconds
    max_retries=10,  # Increase the number of retries
    retry_on_timeout=True  # Enable retry on timeout
)

# create an ingest pipeline to convert book description to a vector
def create_ingest_pipeline():
    resp = es.ingest.put_pipeline(
        id = "text-embedding",
        description = "converts book description text to a vector",
        processors = [
        {
            "inference": {
                "model_id": "sentence-transformers__msmarco-minilm-l-12-v3",
                "target_field": "text_embedding",
                "field_map": {
                "text": "text_field"
                }
            }
            }
        ],
        on_failure = [
            {
            "set": {
                "description": "Index document to 'failed-<index>'",
                "field": "_index",
                "value": "failed-{{{_index}}}"
            }
            },
            {
            "set": {
                "description": "Set error message",
                "field": "ingest.failure",
                "value": "{{_ingest.on_failure_message}}"
            }
            }
        ],
    )
    logger.info(resp)

def create_books_index():
    mappings = {
            "mappings": {
                "properties": {
                    "book_title": {"type": "text"},
                    "author_name": {"type": "text"},
                    "rating_score": {"type": "float"},
                    "rating_votes": {"type": "integer"},
                    "review_number": {"type": "integer"},
                    "text_field": {"type": "text"},
                    "genres": {"type": "keyword"},
                    "year_published": {"type": "integer"},
                    "url": {"type": "text"},
                }
            }
        }
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=mappings)
        print(f"Index '{INDEX_NAME}' created.")
    else:
        print(f"Index '{INDEX_NAME}' already exists. Deleting and recreating the index.")
        es.indices.delete(index=INDEX_NAME)
        es.indices.create(index=INDEX_NAME, body=mappings)

def bulk_ingest_books():
    chunk_size = 100

    file_path = "../data/books_listed_genres.json"
    
    with open(file_path, 'r') as file:
        books = json.load(file)
    
    actions = [
        {
            "_index": INDEX_NAME,
            "_id": book.get("id", None),
            "_source": book
        }
        for book in books
    ]
    

    try:
        success, failed = 0, 0
        for ok, action in helpers.parallel_bulk(es, actions, 
                                                thread_count=4, 
                                                # pipeline="text-embedding",
                                                chunk_size=chunk_size):
            if ok:
                success += 1
            else:
                failed += 1
        logger.info(f"Successfully ingested {success} books into the '{INDEX_NAME}' index.")
        if failed > 0:
            logger.error(f"Failed to ingest {failed} books.")
    except BulkIndexError as e:
        logger.error(f"Error occurred while ingesting books: {e}")
        logger.error(e.errors)


def check_document_count():
    INDEX_NAME = "books_parallel"
    response = es.count(index=INDEX_NAME)
    logger.info(f"Document count in '{INDEX_NAME}' index: {response['count']}")
    # try:
    #     helpers.parallel_bulk(es, actions, thread_count=4)
    #     print(f"Successfully ingested {len(actions)} books into the '{INDEX_NAME}' index.")
    # except BulkIndexError as e:
    #     logger.error(f"Error occurred while ingesting books: {e}")
    #     logger.error(e.errors)

# create_ingest_pipeline()
create_books_index()
bulk_ingest_books()
check_document_count()