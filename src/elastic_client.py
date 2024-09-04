import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv(override=True)

ELASTIC_ENDPOINT = os.environ.get("ELASTIC_ENDPOINT")
ELASTIC_API_KEY = os.environ.get("ELASTIC_API_KEY")

es = Elasticsearch(
    hosts=ELASTIC_ENDPOINT,
    api_key=ELASTIC_API_KEY,
)
