import json
import logging
import os

import pandas as pd


class DataStatistics:
    """
    Class making it possible to get general statistics about some specific data. Can be used to get an overview of
    the data before processing it further.

    Parameters
    ----------
    save_path : str
        The path to where the preprocessed data should be saved.
    grouped_dataframes : dict, optional
        Dictionary from locations to dataframes that represent the run-time version of the preprocessed data in the
        location (the default is None, meaning that the data should be collected from the "data_folder" parameter).
    data_folder : str, optional
        The path to the folder containing the data that should be preprocessed (the default is None, meaning that
        the data is given directly in the "grouped_dataframes" parameter).
    """
    def __init__(self, save_path, grouped_dataframes=None, data_folder=None):
        self.save_path = save_path
        self.data_folder = data_folder

        # Manually loading grouped dataframes if they were not given.
        if grouped_dataframes is None:
            self.grouped_dataframes = self.__load_grouped_data()
        else:
            self.grouped_dataframes = grouped_dataframes

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
        total_dataframe = pd.concat(self.__combine_dict_values(self.grouped_dataframes), ignore_index=True)

        statistics = {
            "time_frame": self.__get_time_frame(total_dataframe),
            **self.__get_measurement_statistics(total_dataframe),
            "location_statistics": self.__get_location_statistics()
        }

        with open(f"{self.save_path}/statistics.json", "w+") as jsonfile:
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

    def __get_location_statistics(self):
        location_statistics = {
            "location_count": len(self.grouped_dataframes)
        }
        for location, location_dataframes in self.grouped_dataframes.items():
            total_dataframe = pd.concat(location_dataframes, ignore_index=True)

            location_statistics[location] = {
                "time_frame": self.__get_time_frame(total_dataframe),
                **self.__get_measurement_statistics(total_dataframe),
            }

        return location_statistics

    # Combine values in dict to one list. The dict must only have values of type list.
    @staticmethod
    def __combine_dict_values(dictionary):
        combined = []
        for key, value in dictionary.items():
            combined.extend(value)

        return combined
