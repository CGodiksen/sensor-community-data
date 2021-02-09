import collections
import pandas as pd


class Statistics:
    def __init__(self, dataframes, save_path):
        self.dataframes = dataframes
        self.save_path = save_path

    # Create a JSON file with statistics about the data in the given dataframes.
    def create_statistics_file(self):
        pass

    @staticmethod
    def __get_measurement_count(dataframes):
        measurement_count = 0
        for df in dataframes:
            measurement_count += len(df.index)

    @staticmethod
    def __get_sensor_count(dataframes):
        sensors = []
        for df in dataframes:
            sensors.append(df.at[0, "sensor_id"])

        return len(list(set(sensors)))

    @staticmethod
    def __get_time_frame(dataframes):
        earliest_date = None
        latest_date = None

        for df in dataframes:
            date = df.at[0, "timestamp"][:10]

            if date < earliest_date:
                earliest_date = date
            elif date > latest_date:
                latest_date = date

    @staticmethod
    def __get_location_statistics(dataframes):
        # Split the data into cities.

    # Return a dictionary from locations to dataframes related to the specific locations.
    @staticmethod
    def __group_by_location(dataframes):
        grouped_dataframes = collections.defaultdict(list)

        for df in dataframes:
            grouped_dataframes[df.at[0, "location"]].append(df)

        return grouped_dataframes
