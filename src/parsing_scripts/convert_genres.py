import json

# Read the contents of books_remove_ot.json
with open('../data/books.json', 'r') as file:
    books = json.load(file)

# remove duplicate entries in the books.json file
books = list({book['Book_Title']: book for book in books}.values())


# convert json object to NDJSON format. name as books.json
with open('../data/dedup_books.json', 'w') as file:
    for book in books:
        json.dump(book, file)
        file.write('\n')
 

    

# # Save the updated books object as books_with_genre.json
# with open('../data/books_with_genre_objs.json', 'w') as file:
#     json.dump(books, file, indent=2)