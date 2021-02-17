import logging
import time
from datetime import date
from pathlib import Path

from scraper import Scraper
from preprocessor import Preprocessor
from data_statistics import DataStatistics

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = Path(f"data/{int(time.time())}")

    scraper = Scraper(path, ["P1", "P2"], start_date=date(2017, 1, 1), end_date=date(2017, 1, 3),
                      sensor_types=["sds011"], save_data=False)
    scraper.start()

    preprocessor = Preprocessor(f"{path}_preprocessed", dataframes=scraper.dataframes,
                                resample_freq="5T")
    preprocessor.start()

    data_statistics = DataStatistics(f"{path}_preprocessed", grouped_dataframes=preprocessor.final_grouped_dataframes)
    data_statistics.create_statistics_file()
