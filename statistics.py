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
        # Also get monthly count.
        pass

    @staticmethod
    def __get_sensor_count(dataframes):


    @staticmethod
    def __get_time_frame(dataframes):

    @staticmethod
    def __get_city_statistics(dataframes):
        pass
