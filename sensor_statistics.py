import collections
import json
import pandas as pd


class SensorStatistics:
    def __init__(self, dataframes, save_path, measurements):
        self.dataframes = dataframes
        self.save_path = save_path
        self.measurements = measurements

        self.__get_measurement_statistics(dataframes)

    # Create a JSON file with statistics about the data in the given dataframes.
    def create_statistics_file(self):
        print("Creating statistics file...")
        statistics = {
            "time_frame": self.__get_time_frame(self.dataframes),
            "sensor_count": self.__get_sensor_count(self.dataframes),
            **self.__get_measurement_statistics(self.dataframes),
            "location_statistics": self.__get_location_statistics(self.dataframes)
        }

        with open(f"{self.save_path}/statistics.json", "w+") as jsonfile:
            json.dump(statistics, jsonfile)

    @staticmethod
    def __get_sensor_count(dataframes):
        sensors = []
        for df in dataframes:
            sensors.append(df.at[0, "sensor_id"])

        return len(list(set(sensors)))

    @staticmethod
    def __get_time_frame(dataframes):
        earliest_date = dataframes[0].at[0, "timestamp"][:10]
        latest_date = ""

        for df in dataframes:
            date = df.at[0, "timestamp"][:10]

            if date < earliest_date:
                earliest_date = date
            elif date > latest_date:
                latest_date = date

        return f"{earliest_date} - {latest_date}"

    # Return a dictionary with a key for each measurement. The value is a dict of statistics about the key measurement.
    def __get_measurement_statistics(self, dataframes):
        measurement_statistics = {}
        total_dataframe = pd.concat(dataframes)

        description = total_dataframe.describe()
        statistic_names = list(description.index)

        for measurement in self.measurements:
            measurement_statistics[measurement] = dict(zip(statistic_names, description[measurement]))

        return measurement_statistics

    def __get_location_statistics(self, dataframes):
        # Split the data into cities.
        grouped_dataframes = self.__group_by_location(dataframes)

        location_statistics = {}
        for location, location_dataframes in grouped_dataframes.items():
            location_statistics[location] = {
                "time_frame": self.__get_time_frame(location_dataframes),
                "sensor_count": self.__get_sensor_count(location_dataframes),
                **self.__get_measurement_statistics(location_dataframes),
            }

        return location_statistics

    # Return a dictionary from locations to dataframes related to the specific locations.
    @staticmethod
    def __group_by_location(dataframes):
        grouped_dataframes = collections.defaultdict(list)

        for df in dataframes:
            grouped_dataframes[df.at[0, "location"]].append(df)

        return grouped_dataframes