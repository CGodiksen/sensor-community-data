import collections
import json
import logging
from pathlib import Path

import pandas as pd
import requests


# TODO: Data cleaning
class Preprocessor:
    def __init__(self, data_folder, combine_city_data=False, resample_freq=None):
        self.data_folder = data_folder
        self.combine_city_data = combine_city_data
        self.resample_freq = resample_freq
        self.dataframes = self.__get_dataframes()

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
            dataframes.append(df)

        logging.info(f"Loaded {len(dataframes)} csv files into dataframes")
        return dataframes

    def start(self):
        self.__save_preprocessing_settings()

        # Doing preprocessing that should be applied to each dataframe individually.
        for df in self.dataframes:
            self.__simplify_location(df)
            df["timestamp"] = pd.to_datetime(df["timestamp"], infer_datetime_format=True)
        logging.info("Simplified location columns and parsed timestamp column into datetime")

        grouped_dataframes = self.__group_dataframes_by_location()

        for location, location_dataframes in grouped_dataframes.items():
            if self.combine_city_data:
                location_dataframes = self.__combine_city_dataframes(location, location_dataframes)

            self.__dataframes_to_csv(location, location_dataframes)

        # Saving the potentially changed cache to persistent storage.
        with open("location_cache.json", "w") as cachefile:
            json.dump(self.location_cache, cachefile)

    # Creating a settings file specifying which settings are used for data preprocessing.
    def __save_preprocessing_settings(self):
        path = Path(f"{self.data_folder}_preprocessed/")
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = {"combine_city_data": self.combine_city_data, "resample_frequency": self.resample_freq}
            json.dump(settings, jsonfile, default=str)

    # Uses reverse geocoding to replace the "location", "lat" and "lon" columns with city-country.
    def __simplify_location(self, df):
        location_id = df.at[0, "location"]
        lat = df.at[0, "lat"]
        lng = df.at[0, "lon"]

        if not pd.isnull(lat) and not pd.isnull(lng):
            location = self.__get_city_country(str(location_id), lat, lng)
        else:
            location = ""

        del df["lat"]
        del df["lon"]
        df.loc[:, "location"] = location
        logging.info(f"Simplified {location_id}, {lat}, {lng} to {location}")

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

    # Return a dictionary from locations to dataframes related to the specific locations.
    def __group_dataframes_by_location(self):
        grouped_dataframes = collections.defaultdict(list)

        for df in self.dataframes:
            grouped_dataframes[df.at[0, "location"]].append(df)
            del df["location"]

        return grouped_dataframes

    def __combine_city_dataframes(self, location, dataframes):
        df = pd.concat(dataframes, ignore_index=True)
        df.sort_values("timestamp", inplace=True)

        if self.resample_freq:
            df = df.resample(self.resample_freq, on="timestamp").mean()
            df.reset_index(level=0, inplace=True)

        df.attrs["file_name"] = location

        return [df]

    # Writing each dataframe to the final folder structure.
    def __dataframes_to_csv(self, location, dataframes):
        path = Path(f"{self.data_folder}_preprocessed/{location}/")
        path.mkdir(parents=True, exist_ok=True)

        for df in dataframes:
            df.to_csv(f"{path.as_posix()}/{df.attrs['file_name']}.csv", index=False)
        logging.info(f"Saved data from {location} to persistent storage")
