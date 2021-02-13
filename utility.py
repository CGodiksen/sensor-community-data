import collections


# Return a dictionary from locations to dataframes related to the specific locations.
def group_by_location(dataframes):
    grouped_dataframes = collections.defaultdict(list)

    for df in dataframes:
        grouped_dataframes[df.at[0, "location"]].append(df)

    return grouped_dataframes
