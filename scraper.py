import json
import logging
import time
from datetime import date, timedelta
from multiprocessing.dummy import Pool
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

import utility
from sensor_statistics import SensorStatistics


# TODO: If the used config matches an existing config file then don't download already downloaded files.
# TODO: Data cleaning
class Scraper:
    def __init__(self, start_date=date(2015, 10, 1), end_date=date.today(), sensor_types=None, sensor_ids=None,
                 locations=None, measurements=None, remove_indoor=True, combine_city_data=True, create_statistics=True):
        # Columns that are constant for all files from sensor community.
        self.common_columns = ["sensor_id", "sensor_type", "location", "lat", "lon", "timestamp"]
        self.url = "https://archive.sensor.community/"

        self.start_date = start_date
        self.end_date = end_date
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.locations = locations
        self.measurements = measurements
        self.remove_indoor = remove_indoor
        self.combine_city_data = combine_city_data
        self.create_statistics = create_statistics

        with open("location_cache.json", "r") as cachefile:
            self.location_cache = json.load(cachefile)

        with open("config.json", "r") as configfile:
            self.api_key = json.load(configfile)['maps_api_key']

        self.start()

    def start(self):
        folder_path = self.__save_data_settings()

        # Retrieving the data from the online archive.
        date_urls = self.__get_date_urls()
        file_urls = self.__get_file_urls(date_urls)

        dataframes = Pool().map(self.__process_file, file_urls)
        self.__dataframes_to_csv(dataframes, folder_path)

        if self.create_statistics and dataframes:
            SensorStatistics(dataframes, folder_path, self.measurements).create_statistics_file()

        # Saving the potentially changed cache to persistent storage.
        with open("location_cache.json", "w") as cachefile:
            json.dump(self.location_cache, cachefile)

    # Return list of urls corresponding to the days which should be scraped from.
    def __get_date_urls(self):
        delta = self.end_date - self.start_date

        return [f"{self.url}{str(self.start_date + timedelta(days=i))}" for i in range(delta.days + 1)]

    # Return a list of the files that should be scraped, gathered from each date url.
    def __get_file_urls(self, date_urls):
        file_urls = []

        for date_url in date_urls:
            logging.info(f"Retrieving file urls from {date_url}...")
            date_html = requests.get(date_url).text
            soup = BeautifulSoup(date_html, features="html.parser")

            file_urls.extend([f"{date_url}/{a['href']}" for a in soup.find_all('a', href=True)])

        file_urls = self.__remove_unwanted_files(file_urls)
        logging.info(f"Retrieved {len(file_urls)} file urls")

        return file_urls

    def __remove_unwanted_files(self, file_urls):
        # Removing urls that do not link to a CSV file.
        file_urls = list(filter(lambda file_url: file_url.endswith(".csv"), file_urls))

        # Removing urls to data that does not use one of the specified sensor types, if any were specified.
        if self.sensor_types:
            file_urls = list(filter(
                lambda file_url: any(sensor_type in file_url for sensor_type in self.sensor_types), file_urls))

        # Doing the same as above but for sensor ids.
        if self.sensor_ids:
            file_urls = list(filter(
                lambda file_url: any(f"sensor_{sensor_id}" in file_url for sensor_id in self.sensor_ids), file_urls))

        if self.remove_indoor:
            file_urls = list(filter(lambda file_url: "indoor" not in file_url, file_urls))

        return file_urls

    # Fully processing a single file, including reading it from the archive, cleaning the data and saving it to storage.
    def __process_file(self, file_url):
        df = self.__read_csv_helper(file_url)
        self.__simplify_location(df)

        return df

    def __read_csv_helper(self, file_url):
        logging.info(f"Converting {file_url} to a dataframe")

        return pd.read_csv(file_url, sep=";", usecols=self.common_columns + self.measurements)

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

    # Return a string with the format "city-country" based on the given latitude and longitude.
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

    # Writing each dataframe to the final folder structure.
    def __dataframes_to_csv(self, dataframes, folder_path):
        if self.combine_city_data:
            grouped_dataframes = utility.group_by_location(dataframes)

            for location, dataframes in grouped_dataframes.items():
                df = pd.concat(dataframes, ignore_index=True)
                df.sort_values("timestamp")
                self.__to_csv_helper(df, folder_path)
        else:
            for df in dataframes:
                self.__to_csv_helper(df, folder_path)

    @staticmethod
    def __to_csv_helper(df, folder_path):
        location = df.at[0, "location"]

        # Removing data that has no location.
        if location:
            sensor_id = df.at[0, "sensor_id"]
            data_date = df.at[0, "timestamp"][:10]

            path = Path(f"{folder_path}/{location}/")
            path.mkdir(parents=True, exist_ok=True)

            df.to_csv(f"{folder_path}/{location}/{sensor_id}_{data_date}.csv", index=False)

    # Creating a settings file specifying which settings are used for data retrieval.
    def __save_data_settings(self):
        path = Path(f"data/{int(time.time())}/")
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = self.__dict__.copy()
            del settings["url"]

            json.dump(settings, jsonfile, default=str)

        return path
