This is a companion repository for the article *Navigating an Elastic Vector Database* found [Navigating an Elastic Vector Database](). This contains all of the necessary instructions to operate a vector database with Elasticsearch


Repository contents:

- `src/`: location where all scripts are stored

- `src/parsing_scripts`: original scripts used to convert a multi-file folder of book csv files into a single `books.json` file.

- `src/upload_books`: creates an index within Elasticsearch and uploads book documents in bulk and individually. Also creates an ingest pipeline to embed vectors as they are indexed in Elasticsearch

- 'ingest pipeline'
