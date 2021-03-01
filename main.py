import logging
import time
from datetime import date

from data_statistics import DataStatistics
from preprocessor import Preprocessor
from scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = f"data/{int(time.time())}"

    scraper = Scraper(["P1", "P2"], start_date=date(2021, 1, 1), end_date=date(2021, 1, 1), sensor_types=["sds011"], sensor_ids=[140])
    scraper.start()

    preprocessor = Preprocessor(f"{path}_preprocessed", dataframes=scraper.dataframes, combine_city_data=True,
                                resample_freq="60T", add_lockdown_info=True)
    preprocessor.start()

    data_statistics = DataStatistics(f"{path}_preprocessed", grouped_dataframes=preprocessor.final_grouped_dataframes)
    data_statistics.create_statistics_file()
