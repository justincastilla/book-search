import json

# Read the contents of books_remove_ot.json
with open('../data/books_with_genre_objs.json', 'r') as file:
    books = json.load(file)

# convert json object to NDJSON format. name as books.json

with open('../data/books.json', 'w') as file:
    for book in books:
        json.dump(book, file)
        file.write('\n')
