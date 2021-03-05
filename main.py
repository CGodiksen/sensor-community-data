import logging
import time
from datetime import date, timedelta

from preprocessor import Preprocessor
from scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = f"data/{int(time.time())}"

    start_date = date(2017, 1, 1)
    end_date = date(2017, 1, 3)
    delta = end_date - start_date

    for date in [start_date + timedelta(days=i) for i in range(delta.days + 1)]:
        scraper = Scraper(["P1", "P2"], start_date=date, end_date=date, sensor_types=["sds011"])
        scraper.start()

        preprocessor = Preprocessor(f"{path}_preprocessed", dataframes=scraper.dataframes, combine_city_data=True,
                                    resample_freq="60T", add_lockdown_info=True)
        preprocessor.start()

