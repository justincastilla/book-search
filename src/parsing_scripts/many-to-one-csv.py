import pandas as pd
import os

data_dir = '../data'

# List of CSV files to be combined
csv_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]

# Initialize an empty list to hold the dataframes
dataframes = []

# Loop through the list of files and read each one
for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

# Concatenate all the dataframes into one
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined dataframe to a new CSV file
combined_df.to_csv('combined_file.csv', index=False)
