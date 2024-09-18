if __name__ == "__main__":
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

    INDEX_NAME = f"{os.environ.get('INDEX_NAME')}-local"

    # This function embeds the book descriptions using the sentence-transformers library locally
    # and saves the embedded descriptions to a new file called "books_embedded.json".
    # Use the optional parameter small_sample=True to embed a small sample of books for testing.
    def embed_descriptions(file_path="../data/books.json", output_file="../data/books_embedded.json"):

        model = SentenceTransformer("sentence-transformers/msmarco-MiniLM-L-12-v3")
        logger.info("Model loaded for embedding...")

        with open(file_path, "r") as file:
            books = json.load(file)
        logger.info(f"Loading books from file {file_path} for embedding...")
        
        book_descriptions = [book["book_description"] for book in books]
        embedded_books = []

        pool = model.start_multi_process_pool()

        logger.info("Starting multi-process pool for embedding...")
        embedded_books = model.encode_multi_process(book_descriptions, pool)
        logger.info("Embeddings computed. Shape:", embedded_books.shape)
        
        model.stop_multi_process_pool(pool)
        logger.info("Stopping multi-process pool...")

        new_books = []

        for i, book in enumerate(books):
            book["description_embedding"] = embedded_books[i].tolist()
            new_books.append(book)
        logger.info("Embeddings added to books.")


        # Write the embedded books to a new array of documents named books_embeded.json
        with open(output_file, "w") as file:
            file.write('[')
            for i, book in enumerate(new_books):
                json.dump(book, file)
                if i != len(new_books) - 1:
                    file.write(',')                  
            file.write(']')
        logger.info(f"{len(new_books)} embedded books saved to file: {output_file}.")

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
                    "description_embedding" : {
                        "type": "dense_vector",
                        "dims": 384
                    }
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
            algorithm = "dot_product"
            es.indices.create(index=INDEX_NAME, body=mappings)
            logger.info(f"Index '{INDEX_NAME}' created.")

    def create_one_book(book):

        try:
            resp = es.index(
                index=INDEX_NAME,
                id=book.get("id", None),
                body=book,
            )

            logger.info(f"Successfully indexed book: {resp.get('result', None)}")

        except Exception as e:
            logger.error(f"Error occurred while indexing book: {e}")

    def bulk_ingest_books(file_path="../data/books_embedded.json"):

        with open(file_path, "r") as file:
            books = json.load(file)

        actions = [
            {"_index": INDEX_NAME, "_id": book.get("id", None), "_source": book}
            for book in books
        ]

        try:
            helpers.bulk(es, actions, chunk_size=1000)
            logger.info(f"Successfully added {len(actions)} books into the '{INDEX_NAME}' index.")

        except helpers.BulkIndexError as e:
            logger.error(f"Error occurred while ingesting books: {e}")

    embed_descriptions('../data/small_books.json')
    create_books_index()
    bulk_ingest_books()

    embed_descriptions('../data/one_book.json', '../data/one_book_embedded.json')
    with open("../data/one_book.json", "r") as file:
        book = json.load(file)
        create_one_book(book) 
