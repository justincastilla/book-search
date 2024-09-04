import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import logging

load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


CLOUD_ID = os.environ.get("CLOUD_ID")
ELASTIC_USERNAME = os.environ.get("ELASTIC_USERNAME")
ELASTIC_PASSWORD = os.environ.get("ELASTIC_PASSWORD")

INDEX_NAME = os.environ.get("INDEX_NAME")
MODEL_ID = os.environ.get("MODEL_ID")

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),  # type: ignore
    request_timeout=60,  # Increase the timeout to 60 seconds
    max_retries=10,  # Increase the number of retries
    retry_on_timeout=True,  # Enable retry on timeout
)


def execute_query(query_string):
    search_result = es.search(
        index=INDEX_NAME,
        knn={
            "field": "description_embedding",
            "k": 10,
            "num_candidates": 50,
            "query_vector_builder": {
                "text_embedding": {"model_id": MODEL_ID, "model_text": query_string}
            },
        },
    )

    return search_result


def print_results(search_result):
    for hit in search_result["hits"]["hits"]:
        print(f"Book: {hit['_source']['book_title']}")
        print(f"Author: {hit['_source']['author_name']}")
        print(f"Description: {hit['_source']['book_description']}")
        print(f"Score: {hit['_score']}")
        print("")


results = execute_query("I want to read a book about zombies fighting aliens.")
print_results(results)
