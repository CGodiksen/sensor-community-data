import json
import logging

import pandas as pd

import utility


class SensorStatistics:
    def __init__(self, dataframes, save_folder, measurements):
        self.dataframes = dataframes
        self.save_folder = save_folder
        self.measurements = measurements

    # Create a JSON file with statistics about the data in the given dataframes.
    def create_statistics_file(self):
        logging.info("Creating statistics file...")
        total_dataframe = pd.concat(self.dataframes, ignore_index=True)

        statistics = {
            "time_frame": self.__get_time_frame(total_dataframe),
            "sensor_count": self.__count_unique(self.dataframes, "sensor_id"),
            **self.__get_measurement_statistics(total_dataframe),
            "location_statistics": self.__get_location_statistics(self.dataframes)
        }

        with open(f"data/{self.save_folder}/metadata/statistics.json", "w+") as jsonfile:
            json.dump(statistics, jsonfile)

    @staticmethod
    def __count_unique(dataframes, column_name):
        all_column = []
        for df in dataframes:
            all_column.append(df.at[0, column_name])

        return len(list(set(all_column)))

    @staticmethod
    def __get_time_frame(total_dataframe):
        total_dataframe.sort_values("timestamp", inplace=True)

        earliest_date = total_dataframe["timestamp"].iloc[0].date()
        latest_date = total_dataframe["timestamp"].iloc[-1].date()

        return f"{earliest_date} - {latest_date}"

    # Return a dictionary with a key for each measurement. The value is a dict of statistics about the key measurement.
    def __get_measurement_statistics(self, total_dataframe):
        measurement_statistics = {}

        description = total_dataframe.describe()
        statistic_names = list(description.index)

        for measurement in self.measurements:
            measurement_statistics[measurement] = dict(zip(statistic_names, description[measurement]))

        return measurement_statistics

    def __get_location_statistics(self, dataframes):
        # Split the data into cities.
        grouped_dataframes = utility.group_by_location(dataframes)

        location_statistics = {
            "location_count": self.__count_unique(self.dataframes, "location")
        }
        for location, location_dataframes in grouped_dataframes.items():
            total_dataframe = pd.concat(location_dataframes, ignore_index=True)

            location_statistics[location] = {
                "time_frame": self.__get_time_frame(total_dataframe),
                "sensor_count": self.__count_unique(location_dataframes, "sensor_id"),
                **self.__get_measurement_statistics(total_dataframe),
            }

        return location_statistics
