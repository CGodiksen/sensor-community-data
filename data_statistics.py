import json
import logging
import os

import pandas as pd


class DataStatistics:
    def __init__(self, data_folder):
        self.data_folder = data_folder
        self.grouped_dataframes = self.__load_grouped_data()

    # Loads the CSV files from each folder in the data folder into a separate group with the folder name as the key.
    def __load_grouped_data(self):
        grouped_dataframes = {}

        for folder in next(os.walk(self.data_folder))[1]:
            path = f"{self.data_folder}/{folder}/"
            grouped_dataframes[folder] = [pd.read_csv(f"{path}/{file}") for file in os.listdir(path)]

        return grouped_dataframes

    # Create a JSON file with statistics about the data in the given dataframes.
    def create_statistics_file(self):
        logging.info("Creating statistics file...")
        total_dataframe = pd.concat(self.dataframes, ignore_index=True)

        statistics = {
            "time_frame": self.__get_time_frame(total_dataframe),
            **self.__get_measurement_statistics(total_dataframe),
            "location_statistics": self.__get_location_statistics(self.dataframes)
        }

        with open(f"{self.data_folder}/statistics.json", "w+") as jsonfile:
            json.dump(statistics, jsonfile)

    @staticmethod
    def __get_time_frame(total_dataframe):
        total_dataframe["timestamp"] = pd.to_datetime(total_dataframe["timestamp"], infer_datetime_format=True)
        total_dataframe.sort_values("timestamp", inplace=True)

        earliest_date = total_dataframe["timestamp"].iloc[0].date()
        latest_date = total_dataframe["timestamp"].iloc[-1].date()

        return f"{earliest_date} - {latest_date}"

    # Return a dictionary with a key for each measurement. The value is a dict of statistics about the key measurement.
    @staticmethod
    def __get_measurement_statistics(total_dataframe):
        measurement_statistics = {}

        description = total_dataframe.describe()
        statistic_names = list(description.index)

        measurement_columns = total_dataframe.columns.values.tolist()
        measurement_columns.remove("timestamp")
        for measurement in measurement_columns:
            measurement_statistics[measurement] = dict(zip(statistic_names, description[measurement]))

        return measurement_statistics

    def __get_location_statistics(self, dataframes):
        # Split the data into cities.
        grouped_dataframes = utility.group_by_location(dataframes)

        location_statistics = {
            "location_count": len(list(os.walk(self.data_folder))[1])
        }
        for location, location_dataframes in grouped_dataframes.items():
            total_dataframe = pd.concat(location_dataframes, ignore_index=True)

            location_statistics[location] = {
                "time_frame": self.__get_time_frame(total_dataframe),
                **self.__get_measurement_statistics(total_dataframe),
            }

        return location_statistics

    # Return a
    def __get_dataframes_by_location(self):
        pass


test = DataStatistics("data/1613263929_preprocessed")
test.create_statistics_file()
