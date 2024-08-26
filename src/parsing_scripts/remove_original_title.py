import json

# Open the books.json file
with open('../data/books.json', 'r') as file:
    data = json.load(file)

# Remove the "Original_Book_Title" field from each object
for book in data:
    if "Original_Book_Title" in book:
        del book["Original_Book_Title"]

# Save the modified data back to the books.json file
with open('../data/books_remove_ot.json', 'w') as file:
    json.dump(data, file, indent=2)