import collections
import json
import logging
from pathlib import Path

import pandas as pd
import requests
import pycountry


# TODO: Data cleaning
class Preprocessor:
    def __init__(self, save_path, dataframes=None, data_folder=None, combine_city_data=False, resample_freq=None,
                 add_lockdown_info=False):
        self.final_grouped_dataframes = {}

        self.save_path = save_path
        self.data_folder = data_folder
        self.combine_city_data = combine_city_data
        self.resample_freq = resample_freq
        self.add_lockdown_info = add_lockdown_info

        # Manually loading dataframes if they were not given.
        if dataframes is None:
            self.dataframes = self.__get_dataframes()
        else:
            self.dataframes = dataframes

        with open("location_cache.json", "r") as location_cachefile:
            self.location_cache = json.load(location_cachefile)

        with open("config.json", "r") as configfile:
            self.api_key = json.load(configfile)['maps_api_key']

        with open("lockdown_cache.json", "r") as lockdown_cachefile:
            self.lockdown_cache = json.load(lockdown_cachefile)

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

        self.__clean_individual_dataframes()

        grouped_dataframes_location = self.__group_dataframes_by_location(grouped_dataframes_sensor_id,
                                                                          sensor_locations)

        for location, location_dataframes in grouped_dataframes_location.items():
            if self.combine_city_data:
                location_dataframes = self.__combine_city_dataframes(location_dataframes)

            if self.resample_freq:
                location_dataframes = self.__resample_helper(location_dataframes)

            if self.add_lockdown_info:
                self.__add_lockdown_attribute(location, location_dataframes)

            self.__dataframes_to_csv(location, location_dataframes)

        # Saving the potentially changed caches to persistent storage.
        with open("location_cache.json", "w") as location_cachefile:
            json.dump(self.location_cache, location_cachefile)

        with open("lockdown_cache.json", "w") as lockdown_cachefile:
            json.dump(self.lockdown_cache, lockdown_cachefile)

    # Creating a settings file specifying which settings are used for data preprocessing.
    def __save_preprocessing_settings(self):
        path = Path(self.save_path)
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = {"combine_city_data": self.combine_city_data, "resample_frequency": self.resample_freq}
            json.dump(settings, jsonfile, default=str)

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

            return f"{city}_{country}"
        except IndexError:
            return ""

    # Doing preprocessing that should be applied to each dataframe individually.
    def __clean_individual_dataframes(self):
        for df in self.dataframes:
            df["timestamp"] = pd.to_datetime(df["timestamp"], infer_datetime_format=True).dt.tz_localize(None)

            # Removing location information from the data itself since it is now handled as metadata.
            del df["lat"]
            del df["lon"]
            del df["location"]

    def __combine_city_dataframes(self, dataframes):
        combined_dataframes = []
        grouped_dataframes_date = self.__group_dataframes_by_attribute(dataframes, "date")

        for date, date_dataframes in grouped_dataframes_date.items():
            df = pd.concat(date_dataframes, ignore_index=True)
            df.sort_values("timestamp", inplace=True)

            # Reassigning needed metadata attributes that were lost when concatenating.
            df.attrs["file_name"] = date
            df.attrs["date"] = date

            combined_dataframes.append(df)

        return combined_dataframes

    def __resample_helper(self, dataframes):
        resampled_dataframes = []

        for df in dataframes:
            # Extracting some metadata attributes since they are removed when resampling.
            file_name = df.attrs["file_name"]
            date = df.attrs["date"]

            df = df.resample(self.resample_freq, on="timestamp").mean()
            df.reset_index(level=0, inplace=True)

            # Rounding since resampling with mean results in too many decimals for the measurements.
            df = df.round(2)

            resampled_dataframes.append(df)
            df.attrs["file_name"] = file_name
            df.attrs["date"] = date

        return resampled_dataframes

    # Checks if the country was locked down on the specific day and adds the result to the metadata attributes.
    def __add_lockdown_attribute(self, location, dataframes):
        country = location.split("_")[-1]
        alpha_3_code = pycountry.countries.lookup(country).alpha_3

        for df in dataframes:
            if self.__check_lockdown_status(df, alpha_3_code):
                df.attrs["lockdown"] = "_lockdown"

    # Return true if the country was locked down on the specific date of the data.
    def __check_lockdown_status(self, df, alpha_3_code):
        key = f"{df.attrs['date']}_{alpha_3_code}"

        if key in self.lockdown_cache:
            lockdown_status = self.lockdown_cache[key]
        else:
            lockdown_status = self.__request_lockdown_status(df.attrs["date"], alpha_3_code)
            self.lockdown_cache[key] = lockdown_status

        return lockdown_status

    @staticmethod
    def __request_lockdown_status(date, alpha_3_code):
        # Getting the lockdown status of the specific country on the specific date with the Oxford API.
        api_url = f"https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/actions/{alpha_3_code}/{date}"
        api_response = requests.get(api_url).json()

        try:
            # Currently the threshold is whenever the country has any stay at home requirements.
            # The different policy actions and the meaning of the policy values can be seen here:
            # https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/codebook.md
            if api_response["policyActions"][5]["policyValue_actual"] > 0:
                return True
            else:
                return False
        except IndexError:
            return False

    # Writing each dataframe to the final folder structure.
    def __dataframes_to_csv(self, location, dataframes):
        # Saving the dataframes in an attribute so they can be used directly, outside preprocessing.
        self.final_grouped_dataframes[location] = dataframes

        path = Path(f"{self.save_path}/{location.replace('/', '-')}/")
        path.mkdir(parents=True, exist_ok=True)

        for df in dataframes:
            lockdown = df.attrs.get("lockdown", "")
            df.to_csv(f"{path.as_posix()}/{df.attrs['file_name']}{lockdown}.csv", index=False)
        logging.info(f"Saved data from {location} to persistent storage")

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
