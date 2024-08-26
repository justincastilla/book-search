import json

# Read the contents of books_remove_ot.json
with open('../data/books.json', 'r') as file:
    books = json.load(file)


book_set = set()
deduped_books = []
for book in books:
    if book['url'] not in book_set:
        book_set.add(book['url'])
        deduped_books.append(book)
    else:
        continue

# convert book_set to ndjson format as book_set.json    
# with open('../data/book_set.json', 'w') as file:
#     for book in book_set:
#         file.write(book)
#         file.write('\n')

# # convert json object to NDJSON format. name as books.json
with open('../data/dedupe_books.json', 'w') as file:
    for book in deduped_books:
        json.dump(book, file)
        file.write('\n')

