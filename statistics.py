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
    def __get_city_statistics(dataframes):
        pass
