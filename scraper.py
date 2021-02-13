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
class Scraper:
    def __init__(self, start_date=date(2015, 10, 1), end_date=date.today(), sensor_types=None, sensor_ids=None,
                 measurements=None, remove_indoor=True):
        # Columns that are constant for all files from sensor community.
        self.common_columns = ["sensor_id", "sensor_type", "location", "lat", "lon", "timestamp"]
        self.url = "https://archive.sensor.community/"

        self.start_date = start_date
        self.end_date = end_date
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.measurements = measurements
        self.remove_indoor = remove_indoor

    def start(self):
        folder_name = self.__save_scrape_settings()

        # Retrieving the data from the online archive.
        date_urls = self.__get_date_urls()
        file_urls = self.__get_file_urls(date_urls)

        Pool().map(lambda file_url: self.__process_file(file_url, folder_name), file_urls)

    # Creating a settings file specifying which settings are used for data retrieval.
    def __save_scrape_settings(self):
        unique_name = str(int(time.time()))
        path = Path(f"data/{unique_name}/metadata/")
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = self.__dict__.copy()
            del settings["url"]

            json.dump(settings, jsonfile, default=str)

        return unique_name

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
    def __process_file(self, file_url, folder_name):
        df = self.__read_csv_helper(file_url)
        self.__to_csv_helper(df, folder_name)

    def __read_csv_helper(self, file_url):
        logging.info(f"Converting {file_url} to a dataframe")

        return pd.read_csv(file_url, sep=";", usecols=self.common_columns + self.measurements)

    @staticmethod
    def __to_csv_helper(df, folder_name):
        location = df.at[0, "location"]

        # Removing data that has no location.
        if location:
            sensor_id = df.at[0, "sensor_id"]
            date_str = str(df.at[0, "timestamp"].date())

            path = Path(f"data/{folder_name}/{location}/")
            path.mkdir(parents=True, exist_ok=True)

            file_path = f"{path.as_posix()}/{sensor_id}_{date_str}.csv"

            df.to_csv(file_path, index=False)
