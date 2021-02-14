import collections
import logging
import pandas as pd

from pathlib import Path


# Return a dictionary from locations to dataframes related to the specific locations.
def group_by_location(dataframes):
    grouped_dataframes = collections.defaultdict(list)

    for df in dataframes:
        grouped_dataframes[df.at[0, "location"]].append(df)
        del df["location"]

    return grouped_dataframes


# Parse through all csv files in the given data folder and load them into dataframes.
def get_dataframes(data_folder):
    dataframes = []
    for data_file in Path(data_folder).rglob("*.csv"):
        df = pd.read_csv(data_file)

        df.attrs["file_name"] = data_file.stem
        dataframes.append(df)

    logging.info(f"Loaded {len(dataframes)} csv files into dataframes")
    return dataframes
