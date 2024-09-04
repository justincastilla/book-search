import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from elastic_client import es
import logging

load_dotenv(override=True)

INDEX_NAME = os.environ.get("INDEX_NAME")
MODEL_ID = os.environ.get("MODEL_ID")


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


def full_text_search(query_string):
    search_result = es.search(
        index=INDEX_NAME, body={"query": {"match": {"book_description": query_string}}}
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
# results = full_text_search("Dinosaurs")
print_results(results)
