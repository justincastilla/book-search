import pandas as pd
import os

data_dir = '../data'

# Read the CSV file, skipping lines with incorrect field counts
df = pd.read_csv(os.path.join(data_dir, 'books.csv'), on_bad_lines='skip')

# Convert DataFrame to JSON and save it to books.json
df.to_json(os.path.join(data_dir, 'books.json'), orient='records')