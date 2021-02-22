import logging
import time
from datetime import date
from pathlib import Path

from scraper import Scraper
from preprocessor import Preprocessor
from data_statistics import DataStatistics

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = f"data/{int(time.time())}"

    scraper = Scraper(["P1", "P2"], start_date=date(2017, 1, 1), end_date=date(2017, 1, 1),
                      sensor_types=["sds011"], save_path=path)
    scraper.start()

    preprocessor = Preprocessor(f"{path}_preprocessed", dataframes=scraper.dataframes, combine_city_data=True,
                                resample_freq="5T", add_lockdown_info=True)
    preprocessor.start()

    data_statistics = DataStatistics(f"{path}_preprocessed", grouped_dataframes=preprocessor.final_grouped_dataframes)
    data_statistics.create_statistics_file()
