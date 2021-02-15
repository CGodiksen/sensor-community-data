import logging
import time
from datetime import date
from pathlib import Path

from scraper import Scraper
from preprocessor import Preprocessor
from data_statistics import DataStatistics

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = Path(f"data/{int(time.time())}/")

    scraper = Scraper(path, ["P1", "P2"], start_date=date(2019, 1, 1), end_date=date(2020, 12, 31), sensor_types=["sds011"], sensor_ids=[140])
    scraper.start()

    preprocessor = Preprocessor(path, combine_city_data=True, resample_freq="60T")
    preprocessor.start()

    data_statistics = DataStatistics(f"{path}_preprocessed")
    data_statistics.create_statistics_file()
