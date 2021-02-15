import json
import logging
from datetime import date, timedelta
from multiprocessing.dummy import Pool
from pathlib import Path
from itertools import chain

import pandas as pd
import requests
from bs4 import BeautifulSoup


# TODO: If the used config matches an existing config file then don't download already downloaded files.
class Scraper:
    def __init__(self, save_path, measurements, start_date=date(2015, 10, 1), end_date=date.today(), sensor_types=None,
                 sensor_ids=None, remove_indoor=True):
        self.columns = ["sensor_id", "sensor_type", "location", "lat", "lon", "timestamp"] + measurements
        self.url = "https://archive.sensor.community/"

        self.save_path = save_path
        self.start_date = start_date
        self.end_date = end_date
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.remove_indoor = remove_indoor

    def start(self):
        self.__save_scrape_settings()

        # Retrieving the urls containing the wanted data in the online archive.
        date_urls = self.__get_date_urls()
        file_urls = Pool().map(self.__get_file_urls, date_urls)

        # Flattening the list of lists.
        file_urls = list(chain.from_iterable(file_urls))

        Pool().map(lambda file_url: self.__process_file(file_url), file_urls)

    # Creating a settings file specifying which settings are used for data retrieval.
    def __save_scrape_settings(self):
        self.save_path.mkdir(parents=True, exist_ok=True)

        with open(self.save_path.joinpath("settings.json"), "w+") as jsonfile:
            settings = self.__dict__.copy()
            del settings["url"]

            json.dump(settings, jsonfile, default=str)

    # Return list of urls corresponding to the days which should be scraped from.
    def __get_date_urls(self):
        delta = self.end_date - self.start_date

        return [f"{self.url}{str(self.start_date + timedelta(days=i))}" for i in range(delta.days + 1)]

    # Return a list of the files that should be scraped, gathered from the data url.
    def __get_file_urls(self, date_url):
        logging.info(f"Retrieving file urls from {date_url}...")
        date_html = requests.get(date_url).text
        soup = BeautifulSoup(date_html, features="html.parser")

        file_urls = ([f"{date_url}/{a['href']}" for a in soup.find_all('a', href=True)])
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

    # Fully processing a single file, which involves downloading it, modifying it slightly and saving it locally.
    def __process_file(self, file_url):
        df = self.__read_csv_helper(file_url)
        df.dropna(inplace=True)

        # The dataframe will be empty if at least one value was missing in each row.
        if not df.empty:
            self.__to_csv_helper(df)

    def __read_csv_helper(self, file_url):
        logging.info(f"Converting {file_url} to a dataframe")

        return pd.read_csv(file_url, sep=";", usecols=self.columns)

    def __to_csv_helper(self, df):
        sensor_id = df.at[0, "sensor_id"]
        sensor_type = df.at[0, "sensor_type"]
        date_str = df.at[0, "timestamp"][:10]

        path = Path(f"{self.save_path}/{date_str}/")
        path.mkdir(parents=True, exist_ok=True)

        remaining_columns = [x for x in self.columns if x not in ["sensor_id", "sensor_type"]]
        df.to_csv(f"{path.as_posix()}/{date_str}_{sensor_id}_{sensor_type}.csv", index=False, columns=remaining_columns)
