import json
import time
import requests

from datetime import date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self, start_date=date(2015, 10, 1), end_date=date.today(), sensor_types=None, sensor_ids=None,
                 locations=None, measurements=None):
        self.url = "https://archive.sensor.community/"
        self.start_date = start_date
        self.end_date = end_date
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.locations = locations
        self.measurements = measurements

        self.__save_data_settings()
        print(self.__get_file_urls(self.__get_date_urls()))

    # Start the scraper with the given settings.
    def start(self):
        day_urls = self.__get_date_urls()
        # TODO: Find a list of the files from each date that should be downloaded (based on sensor list, avoid indoor)
        # TODO: If the used config matches an existing config file then don't downloaded already downloaded files.
        # TODO: For each file, download it, process it and remove it.
        # TODO: Processing should include removing extra rows (based on measurements list and removing empty rows)
        # TODO: Location, lat and lon should be turned into a city based on reverse geocoding (maps API)
        # TODO: To avoid repeated API usage a location file should be kept that caches locations.
        # TODO: When the file is processed the data should be written to a file.
        # TODO: Make it possible to create a statistics file that contains information about the data.

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

            file_urls.extend([a["href"] for a in soup.find_all('a', href=True)])

        return file_urls

    # Creating a settings file specifying which settings are used for data retrieval.
    def __save_data_settings(self):
        path = Path(f"data/{int(time.time())}/")
        path.mkdir(parents=True, exist_ok=True)

        with open(path.joinpath("settings.json"), "w+") as jsonfile:
            settings = self.__dict__.copy()
            del settings["url"]

            json.dump(settings, jsonfile, default=str)


test = Scraper(end_date=date(2015, 10, 2))
