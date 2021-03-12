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
    """
    Class allowing scraping data from the sensor community data archive. The specific data that should be scraped
    can be configured using the initialization parameters.

    Attributes
    ----------
    columns : list of str
        Columns that should be kept when retrieving the CSV data. This includes common columns and the wanted
        measurements.
    url : str
        The url of the archive website.
    dataframes : list of pd.dataframe
        Dataframes that represent the run-time version of the scraped data. Use this attribute if the data should be
        used directly in the preprocessor.

    Parameters
    ----------
    measurements : list of str
        The specific sensor measurements that should be collected from the data. All other measurements are removed.
    start_date : datetime.date, optional
        Date object specifying the first day to scrape data from (the default is January 10 2015).
    end_date : datetime.date, optional
        Date object specifying the last to to scrape data from (the default is yesterday).
    sensor_types : list of str, optional
        The sensor types that should be scraped from (the default is None, meaning all sensor types will be included).
    sensor_ids : list of int, optional
        The sensors that should be scraped from (the default is None, meaning all sensor ids will be included).
    remove_indoor : bool, optional
        If true, the scraper removes data from indoor sensors (the default is True).
    save_path : str, optional
        The path to where the scraped data should be saved (the default is None, meaning the data is not saved).
    preprocessor : :class:`Preprocessor`, optional
        The preprocessor to pipe the data into (the default is None, meaning the data is not piped anywhere).
    """
    def __init__(self, measurements, start_date=date(2015, 10, 1), end_date=date.today() - timedelta(1),
                 sensor_types=None, sensor_ids=None, remove_indoor=True, save_path=None, preprocessor=None):
        self.columns = ["location", "lat", "lon", "timestamp"] + measurements
        self.url = "https://archive.sensor.community/"
        self.dataframes = []

        self.start_date = start_date
        self.end_date = end_date
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.remove_indoor = remove_indoor
        self.save_path = save_path
        self.preprocessor = preprocessor

    def start(self):
        if self.save_path:
            self.__save_scrape_settings()

        # Retrieving the urls containing the wanted data in the online archive.
        date_urls = self.get_date_urls()
        daily_file_urls = Pool().map(self.get_file_urls, date_urls)

        # If a preprocessor is given we pipe the data directly into the preprocessor daily.
        if self.preprocessor:
            for file_urls in daily_file_urls:
                dataframes = Pool().map(lambda file_url: self.__process_file(file_url), file_urls)
                dataframes = [df for df in dataframes if not df.empty]

                self.preprocessor.dataframes = dataframes
                self.preprocessor.start()
        # If not, we scrape all the data concurrently and make it available to use through the dataframes attribute.
        else:
            # Flattening the list of lists.
            file_urls = list(chain.from_iterable(daily_file_urls))

            self.dataframes = Pool().map(lambda file_url: self.__process_file(file_url), file_urls)
            self.dataframes = [df for df in self.dataframes if not df.empty]

    # Creating a settings file specifying which settings are used for data retrieval.
    def __save_scrape_settings(self):
        path = Path(self.save_path)
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = self.__dict__.copy()
            del settings["url"]

            json.dump(settings, jsonfile, default=str)

    # Return list of urls corresponding to the days which should be scraped from.
    def get_date_urls(self):
        delta = self.end_date - self.start_date

        return [f"{self.url}{str(self.start_date + timedelta(days=i))}" for i in range(delta.days + 1)]

    # Return a list of the files that should be scraped, gathered from the data url.
    def get_file_urls(self, date_url):
        logging.info(f"Retrieving file urls from {date_url}")
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
            file_urls = list(filter(lambda file_url: any(sensor_id == int(file_url.split("_")[3].replace(".csv", ""))
                                                         for sensor_id in self.sensor_ids), file_urls))

        if self.remove_indoor:
            file_urls = list(filter(lambda file_url: "indoor" not in file_url, file_urls))

        return file_urls

    # Fully processing a single file, which involves downloading it, modifying it slightly and saving it locally.
    def __process_file(self, file_url):
        df = self.__read_csv_helper(file_url)
        df.dropna(inplace=True)

        # The dataframe will be empty if at least one value was missing in each row.
        if not df.empty and self.save_path:
            self.__to_csv_helper(df)

        return df

    def __read_csv_helper(self, file_url):
        logging.info(f"Converting {file_url} to a dataframe")

        df = pd.read_csv(file_url, sep=";", usecols=self.columns)

        # Removing the website and ".csv" from the url with list slicing to get the file name only.
        split_file_name = file_url[44:-4].split("_")

        # Extracting metadata about the dataframe so it can be used to save the data and in preprocessing.
        df.attrs["date"] = split_file_name[0]
        df.attrs["sensor_type"] = split_file_name[1]
        df.attrs["sensor_id"] = split_file_name[3]
        df.attrs["file_name"] = f"{df.attrs['date']}_{df.attrs['sensor_id']}_{df.attrs['sensor_type']}"

        return df

    def __to_csv_helper(self, df):
        path = Path(f"{self.save_path}/{df.attrs['date']}/")
        path.mkdir(parents=True, exist_ok=True)
        file_path = f"{path.as_posix()}/{df.attrs['file_name']}.csv"

        logging.info(f"Saving dataframe to {file_path}")
        df.to_csv(file_path, index=False)
