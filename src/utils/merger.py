import os

import pandas as pd


def merge_document(path: str, output_path: str):
    csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(path, file)
        df = pd.read_csv(file_path)
        dataframes.append(df)
    # Concatenate all DataFrames
    merged_df = pd.concat(dataframes, ignore_index=False)
    # Save the merged DataFrame to a single CSV file
    merged_df.to_csv(output_path, index=False)
