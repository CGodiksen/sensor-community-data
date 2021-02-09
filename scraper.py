import json
import time
import requests
import io
import pandas as pd

from datetime import date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self, start_date=date(2015, 10, 1), end_date=date.today(), sensor_types=None, sensor_ids=None,
                 locations=None, measurements=None, remove_indoor=True):
        self.url = "https://archive.sensor.community/"
        self.start_date = start_date
        self.end_date = end_date
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.locations = locations
        self.measurements = measurements
        self.remove_indoor = remove_indoor

        self.__save_data_settings()
        self.start()

    # Start the scraper with the given settings.
    def start(self):
        date_urls = self.__get_date_urls()
        file_urls = self.__get_file_urls(date_urls)

        dataframes = [pd.read_csv(file_url, sep=";") for file_url in file_urls]
        self.__remove_excess_columns(dataframes)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            pass
            # print(dataframes[0])

        self.__reverse_geocode(dataframes)
        # TODO: If the used config matches an existing config file then don't downloaded already downloaded files.
        # TODO: Location, lat and lon should be turned into a city based on reverse geocoding (maps API)
        # TODO: To avoid repeated API usage a location file should be kept that caches locations.
        # TODO: When the file is processed the data should be written to a file.
        # TODO: Make it possible to create a statistics file that contains information about the data.
        # TODO: Implement HTML caching if it is very slow.
        # TODO: Data cleaning

    # Return list of urls corresponding to the days which should be scraped from.
    def __get_date_urls(self):
        delta = self.end_date - self.start_date

        return [f"{self.url}{str(self.start_date + timedelta(days=i))}" for i in range(delta.days + 1)]

    # Return a list of the files that should be scraped, gathered from each date url.
    def __get_file_urls(self, date_urls):
        file_urls = []

        for date_url in date_urls:
            date_html = requests.get(date_url).text
            soup = BeautifulSoup(date_html, features="html.parser")

            file_urls.extend([f"{date_url}/{a['href']}" for a in soup.find_all('a', href=True)])

        file_urls = self.__remove_unwanted_files(file_urls)

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

    # Removing columns from the given dataframes that are not needed based on the specified measurements list.
    def __remove_excess_columns(self, dataframes):
        if self.measurements:
            # Columns that are constant for all files from sensor community.
            common_columns = ["sensor_id", "sensor_type", "location", "lat", "lon", "timestamp"]
            columns_to_keep = common_columns + self.measurements

            for df in dataframes:
                columns_to_remove = [column for column in list(df) if column not in columns_to_keep]
                for column in columns_to_remove:
                    del df[column]

    # Uses reverse geocoding to replace the "location", "lat" and "lon" columns with city/country.
    def __reverse_geocode(self, dataframes):
        maps_api_url = "https://maps.googleapis.com/maps/api/geocode/json?"
        result_type = "&result_type=locality&result_type=political"

        with open("config.json", "r") as configfile:
            key = f"&key={json.load(configfile)['maps_api_key']}"

        for df in dataframes:
            location_id = df.at[0, "location"]
            lat = df.at[0, "lat"]
            lng = df.at[0, "lon"]

            # Checking if the location is cached, if not then retrieving it with reverse geocoding.
            with open("location_cache.json", "r+") as cachefile:
                location_cache = json.load(cachefile)

                if location_id in location_cache:
                    location = location_cache["location_id"]
                else:
                    # Making a request to the Google Maps reverse geocoding API.
                    api_response = requests.get(f"{maps_api_url}latlng={lat},{lng}{result_type}{key}").json()
                    addr_comp = api_response["results"][0]["address_components"]

                    # Extracting the city and country name by filtering on the address component type.
                    city = list(filter(lambda x: x["types"] == ["locality", "political"], addr_comp))[0]["long_name"]
                    country = list(filter(lambda x: x["types"] == ["country", "political"], addr_comp))[0]["long_name"]

                    location = f"{city}-{country}"

                    # Saving the newly retrieved location in the cache.
                    location_cache[location_id] = location
                    json.dump(location_cache, cachefile)

    # Creating a settings file specifying which settings are used for data retrieval.
    def __save_data_settings(self):
        path = Path(f"data/{int(time.time())}/")
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = self.__dict__.copy()
            del settings["url"]

            json.dump(settings, jsonfile, default=str)


test = Scraper(end_date=date(2015, 10, 1), measurements=["P1", "P2"])
