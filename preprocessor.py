import collections
import json
import logging
from pathlib import Path

import pandas as pd
import requests


# TODO: Data cleaning
class Preprocessor:
    def __init__(self, save_path, dataframes=None, data_folder=None, combine_city_data=False, resample_freq=None):
        self.final_grouped_dataframes = {}

        self.save_path = save_path
        self.data_folder = data_folder
        self.combine_city_data = combine_city_data
        self.resample_freq = resample_freq

        # Manually loading dataframes if they were not given.
        if dataframes is None:
            self.dataframes = self.__get_dataframes()
        else:
            self.dataframes = dataframes

        with open("location_cache.json", "r") as cachefile:
            self.location_cache = json.load(cachefile)

        with open("config.json", "r") as configfile:
            self.api_key = json.load(configfile)['maps_api_key']

    # Parse through all csv files in the given data folder and load them into dataframes.
    def __get_dataframes(self):
        dataframes = []
        for data_file in self.data_folder.rglob("*.csv"):
            df = pd.read_csv(data_file)

            df.attrs["file_name"] = data_file.stem
            df.attrs["sensor_id"] = data_file.stem.split("_")[1]
            dataframes.append(df)

        logging.info(f"Loaded {len(dataframes)} csv files into dataframes")
        return dataframes

    def start(self):
        self.__save_preprocessing_settings()

        # Grouping the dataframes by sensor id so the location is only found once per sensor.
        grouped_dataframes_sensor_id = self.__group_dataframes_by_sensor_id()
        sensor_locations = self.__get_sensor_locations(grouped_dataframes_sensor_id)

        self.__clean_individual_dataframes()

        grouped_dataframes_location = self.__group_dataframes_by_location(grouped_dataframes_sensor_id, sensor_locations)

        for location, location_dataframes in grouped_dataframes_location.items():
            if self.combine_city_data:
                location_dataframes = self.__combine_city_dataframes(location, location_dataframes)

            if self.resample_freq:
                location_dataframes = self.__resample_helper(location_dataframes)

            self.__dataframes_to_csv(location, location_dataframes)

        # Saving the potentially changed cache to persistent storage.
        with open("location_cache.json", "w") as cachefile:
            json.dump(self.location_cache, cachefile)

    # Creating a settings file specifying which settings are used for data preprocessing.
    def __save_preprocessing_settings(self):
        path = Path(self.save_path)
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = {"combine_city_data": self.combine_city_data, "resample_frequency": self.resample_freq}
            json.dump(settings, jsonfile, default=str)

    def __group_dataframes_by_sensor_id(self):
        grouped_dataframes_sensor_id = collections.defaultdict(list)

        for df in self.dataframes:
            grouped_dataframes_sensor_id[df.attrs["sensor_id"]].append(df)

        return grouped_dataframes_sensor_id

    # Return a dict with key-value pairs of the format "sensor_id-location".
    def __get_sensor_locations(self, grouped_dataframes_sensor_id):
        sensor_locations = {}
        for sensor_id, sensor_id_dataframes in grouped_dataframes_sensor_id.items():
            df = sensor_id_dataframes[0]
            location_id = df.at[0, "location"]
            lat = df.at[0, "lat"]
            lng = df.at[0, "lon"]

            location = self.__get_city_country(str(location_id), lat, lng)
            logging.info(f"Simplified {location_id}, {lat}, {lng} to {location}")

            sensor_locations[sensor_id] = location

        return sensor_locations

    # Return a string with the format "city-country" based on the given latitude and longitude
    def __get_city_country(self, location_id, lat, lng):
        # Checking if the location is cached, if not then retrieve it with reverse geocoding.
        if location_id in self.location_cache:
            location = self.location_cache[location_id]
        else:
            location = self.__reverse_geocode(lat, lng)

            # Saving the newly retrieved location in the cache.
            self.location_cache[location_id] = location

        return location

    def __reverse_geocode(self, lat, lng):
        logging.info(f"Reverse geocoding {lat}, {lng}")
        maps_api_url = "https://maps.googleapis.com/maps/api/geocode/json?"
        result_type = "&result_type=locality&result_type=political"
        key = f"&key={self.api_key}"

        # Making a request to the Google Maps reverse geocoding API.
        api_response = requests.get(f"{maps_api_url}latlng={lat},{lng}{result_type}{key}").json()

        try:
            address_comp = api_response["results"][0]["address_components"]

            # Extracting the city and country name by filtering on the address component type.
            city = list(filter(lambda x: x["types"] == ["locality", "political"], address_comp))[0]["long_name"]
            country = list(filter(lambda x: x["types"] == ["country", "political"], address_comp))[0]["long_name"]

            return f"{city}-{country}"
        except IndexError:
            return ""

    @staticmethod
    def __group_dataframes_by_location(grouped_dataframes_sensor_id, sensor_locations):
        grouped_dataframes_location = collections.defaultdict(list)

        for sensor_id, sensor_id_dataframes in grouped_dataframes_sensor_id.items():
            grouped_dataframes_location[sensor_locations[sensor_id]].extend(sensor_id_dataframes)

        return grouped_dataframes_location

    # Doing preprocessing that should be applied to each dataframe individually.
    def __clean_individual_dataframes(self):
        for df in self.dataframes:
            df["timestamp"] = pd.to_datetime(df["timestamp"], infer_datetime_format=True).dt.tz_localize(None)

            # Removing location information from the data itself since it is now handled as metadata.
            del df["lat"]
            del df["lon"]
            del df["location"]

    @staticmethod
    def __combine_city_dataframes(location, dataframes):
        df = pd.concat(dataframes, ignore_index=True)
        df.sort_values("timestamp", inplace=True)

        df.attrs["file_name"] = location

        return [df]

    def __resample_helper(self, dataframes):
        resampled_dataframes = []

        for df in dataframes:
            # Extracting the metadata attribute since it is removed when resampling.
            file_name = df.attrs["file_name"]

            df = df.resample(self.resample_freq, on="timestamp").mean()
            df.reset_index(level=0, inplace=True)

            resampled_dataframes.append(df)
            df.attrs["file_name"] = file_name

        return resampled_dataframes

    # Writing each dataframe to the final folder structure.
    def __dataframes_to_csv(self, location, dataframes):
        # Saving the dataframes in an attribute so they can be used directly, outside preprocessing.
        self.final_grouped_dataframes[location] = dataframes

        path = Path(f"{self.save_path}/{location.replace('/', '-')}/")
        path.mkdir(parents=True, exist_ok=True)

        for df in dataframes:
            df.to_csv(f"{path.as_posix()}/{df.attrs['file_name']}.csv", index=False)
        logging.info(f"Saved data from {location} to persistent storage")
