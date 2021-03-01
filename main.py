import logging
import time
from datetime import date

from data_statistics import DataStatistics
from preprocessor import Preprocessor
from scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = f"data/{int(time.time())}"

    #scraper = Scraper(["P1", "P2"], start_date=date(2018, 1, 1), end_date=date(2018, 1, 31), sensor_types=["sds011"])
    #scraper.start()

    #preprocessor = Preprocessor(f"{path}_preprocessed", dataframes=scraper.dataframes, combine_city_data=True,
    #                            resample_freq="60T", add_lockdown_info=True)
    #preprocessor.start()

    data_statistics = DataStatistics(f"data/1614627910_preprocessed", data_folder="data/1614627910_preprocessed")
    data_statistics.create_statistics_file()
