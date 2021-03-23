import os
import collections
import json
import logging
from pathlib import Path
from multiprocessing.dummy import Pool

import pandas as pd
import requests
import pycountry
from hampel import hampel


class Preprocessor:
    """
    Class allowing preprocessing of data scraped from the sensor community data archive. How the data is preprocessed
    can be configured using the initialization parameters.

    Attributes
    ----------
    location_cache : dict
        Dictionary from location ids to the city and country connected to the id. The cache is loaded from persistent
        storage on initialization and saved again when preprocessing is done.
    lockdown_cache : dict
        Dictionary from date/country to the lockdown status of the country on the specific date. The cache is loaded
        from persistent storage on initialization and saved again when preprocessing is done.
    api_key : str
        The API key used to make requests to the Google Maps API, which is used for reverse geocoding.

    Parameters
    ----------
    save_path : str
        The path to where the preprocessed data should be saved.
    dataframes : list of df.dataframe, optional
        Dataframes that represent the run-time version of the scraped data (the default is None, meaning that the data
        should be collected from the data_folder parameter or be piped directly from the scraper).
    data_folder : str, optional
        The path to the folder containing the data that should be preprocessed (the default is None, meaning that
        the data is given directly in the "dataframes" parameter or piped from the scraper).
    combine_city_data : bool, optional
        If true, the data from each city is combined into a single file (the default is False).
    resample_freq : str, optional
        The offset string representing target conversion (the default is None, meaning no resampling is done).
    add_lockdown_info : bool, optional
        If true, a column is added to the data with a 1 if the specific row was collected during a lockdown and a 0
        otherwise (the default is False).
    clean_data : bool, optional
        if true, clean the data using a conventional Hampel filter. (the default is false).
    """
    def __init__(self, save_path, data_folder=None, dataframes=None, combine_city_data=False, resample_freq=None,
                 add_lockdown_info=False, clean_data=False):
        with open("../cache/location_cache.json", "r") as location_cachefile:
            self.location_cache = json.load(location_cachefile)

        with open("../cache/lockdown_cache.json", "r") as lockdown_cachefile:
            self.lockdown_cache = json.load(lockdown_cachefile)

        with open("../config.json", "r") as configfile:
            self.api_key = json.load(configfile)['maps_api_key']

        self.save_path = save_path
        self.combine_city_data = combine_city_data
        self.resample_freq = resample_freq
        self.add_lockdown_info = add_lockdown_info
        self.clean_data = clean_data

        # Manually loading dataframes if a data location was given.
        if data_folder:
            self.data_folder = Path(data_folder)
            self.dataframes = self.__get_dataframes()
        else:
            self.dataframes = dataframes

    # Parse through all csv files in the given data folder and load them into dataframes.
    def __get_dataframes(self):
        dataframes = []
        for data_file in self.data_folder.rglob("*.csv"):
            df = pd.read_csv(data_file)

            split_file_name = data_file.stem.split("_")
            df.attrs["date"] = split_file_name[0]
            df.attrs["sensor_id"] = split_file_name[1]

            df.attrs["file_name"] = data_file.stem
            dataframes.append(df)

        logging.info(f"Loaded {len(dataframes)} csv files into dataframes")
        return dataframes

    def start(self):
        self.__save_preprocessing_settings()

        # Grouping the dataframes by sensor id so the location is only found once per sensor.
        grouped_dataframes_sensor_id = self.__group_dataframes_by_attribute(self.dataframes, "sensor_id")
        sensor_locations = self.__get_sensor_locations(grouped_dataframes_sensor_id)

        Pool().map(self.__clean_dataframe, self.dataframes)

        grouped_dataframes_location = self.__group_dataframes_by_location(grouped_dataframes_sensor_id, sensor_locations)

        for location, location_dataframes in grouped_dataframes_location.items():
            if location:
                logging.info(f"Processing data from {location}")
                if self.add_lockdown_info:
                    self.__add_lockdown_attribute(location, location_dataframes)

                if self.combine_city_data:
                    location_dataframes = self.__combine_city_dataframes(location, location_dataframes)

                if self.resample_freq:
                    location_dataframes = self.__resample_helper(location_dataframes)

                self.__dataframes_to_csv(location, location_dataframes)

        # Saving the potentially changed caches to persistent storage.
        with open("../cache/location_cache.json", "w") as location_cachefile:
            json.dump(self.location_cache, location_cachefile)

        with open("../cache/lockdown_cache.json", "w") as lockdown_cachefile:
            json.dump(self.lockdown_cache, lockdown_cachefile)

    # Creating a settings file specifying which settings are used for data preprocessing.
    def __save_preprocessing_settings(self):
        path = Path(self.save_path)
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = {
                "combine_city_data": self.combine_city_data,
                "resample_frequency": self.resample_freq,
                "add_lockdown_info": self.add_lockdown_info
            }
            json.dump(settings, jsonfile, default=str)

    # Return a dict with key-value pairs of the format "sensor_id-location".
    def __get_sensor_locations(self, grouped_dataframes_sensor_id):
        sensor_locations = {}
        for sensor_id, sensor_id_dataframes in grouped_dataframes_sensor_id.items():
            df = sensor_id_dataframes[0]

            location_id = str(df["location"].iloc[0])
            lat = df["lat"].iloc[0]
            lng = df["lon"].iloc[0]

            location = self.__get_api_value(location_id, self.location_cache, lambda: self.__reverse_geocode(lat, lng))
            logging.info(f"Simplified {location_id}, {lat}, {lng} to {location}")

            sensor_locations[sensor_id] = location.replace("/", "-")

        return sensor_locations

    # Return a string with the format "city_country" based on the given latitude and longitude.
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

            return f"{city}_{country}"
        except IndexError:
            return ""

    # Doing preprocessing that should be applied to each dataframe individually.
    def __clean_dataframe(self, df):
        df["timestamp"] = pd.to_datetime(df["timestamp"], infer_datetime_format=True).dt.tz_localize(None)

        # Removing location information from the data itself since it is now handled as metadata.
        del df["lat"]
        del df["lon"]
        del df["location"]

        if self.clean_data:
            for measurement in [i for i in list(df) if i != "timestamp"]:
                logging.info(f"Applying a hampel filter to {measurement}...")
                df[measurement] = hampel(df[measurement], window_size=7, n=3)

    # Checks if the country was locked down on the specific day and adds the result to the metadata attributes.
    def __add_lockdown_attribute(self, location, dataframes):
        country = location.split("_")[-1]

        try:
            alpha_3_code = pycountry.countries.lookup(country).alpha_3

            for df in dataframes:
                date = df.attrs["date"]
                key = f"{date}_{alpha_3_code}"
                lockdown = 0

                # 2020-01-01 was the first day with any restrictions so no reason to call API if date is before that.
                if date >= "2020-01-01":
                    # The current threshold for what is considered a "lockdown" (any stay at home requirements).
                    if self.__get_api_value(key, self.lockdown_cache, lambda: self.__get_lockdown_status(date, alpha_3_code)) > 0:
                        lockdown = 1

                df["lockdown"] = lockdown

        except LookupError:
            logging.warning(f"No alpha 3 code could be found for {country}")
            for df in dataframes:
                df["lockdown"] = 0

    @staticmethod
    def __get_lockdown_status(date, alpha_3_code):
        # Getting the lockdown status of the specific country on the specific date with the Oxford API.
        api_url = f"https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/actions/{alpha_3_code}/{date}"
        api_response = requests.get(api_url).json()

        try:
            # Returning the policy value for the "Stay at home requirements" policy type.
            # The different policy types and the meaning of the policy values can be seen here:
            # https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/codebook.md
            return api_response["policyActions"][5]["policyvalue_actual"]
        except IndexError:
            return 0

    @staticmethod
    def __combine_city_dataframes(location, city_dataframes):
        df = pd.concat(city_dataframes, ignore_index=True)
        df.sort_values("timestamp", inplace=True)

        df.attrs["file_name"] = location

        return [df]

    def __resample_helper(self, dataframes):
        resampled_dataframes = []

        for df in dataframes:
            # Extracting some metadata attributes since they are removed when resampling.
            file_name = df.attrs["file_name"]

            df = df.resample(self.resample_freq, on="timestamp").mean()
            df.reset_index(level=0, inplace=True)

            # Rounding since resampling with mean results in too many decimals for the measurements.
            df = df.round(2)

            resampled_dataframes.append(df)
            df.attrs["file_name"] = file_name

        return resampled_dataframes

    # Writing each dataframe to the final folder structure.
    def __dataframes_to_csv(self, location, dataframes):
        if self.combine_city_data:
            path = Path(f"{self.save_path}")
        else:
            path = Path(f"{self.save_path}/{location}/")
            path.mkdir(parents=True, exist_ok=True)

        for df in dataframes:
            file_path = f"{path.as_posix()}/{df.attrs['file_name']}.csv"

            # Appending to the data if the file already exists.
            if not os.path.isfile(file_path):
                df.to_csv(file_path, index=False, header="column_names")
            else:
                df.to_csv(file_path, mode="a", index=False, header=False)
        logging.info(f"Saved data from {location} to persistent storage")

    # Tries to retrieve value from cache, if not possible then retrieves it with the given callable api_func.
    @staticmethod
    def __get_api_value(key, cache, api_func):
        if key in cache:
            value = cache[key]
        else:
            value = api_func()
            # Saving the newly retrieved value in the cache.
            cache[key] = value

        return value

    @staticmethod
    def __group_dataframes_by_attribute(dataframes, attribute):
        grouped_dataframes = collections.defaultdict(list)

        for df in dataframes:
            grouped_dataframes[df.attrs[attribute]].append(df)

        return grouped_dataframes

    @staticmethod
    def __group_dataframes_by_location(grouped_dataframes_sensor_id, sensor_locations):
        grouped_dataframes_location = collections.defaultdict(list)

        for sensor_id, sensor_id_dataframes in grouped_dataframes_sensor_id.items():
            grouped_dataframes_location[sensor_locations[sensor_id]].extend(sensor_id_dataframes)

        return grouped_dataframes_location
