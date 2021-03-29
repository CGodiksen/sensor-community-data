import logging
import time
from datetime import date

from sensor_community_data.preprocessor import Preprocessor
from sensor_community_data.scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = f"../data/{int(time.time())}"

    preprocessor = Preprocessor(f"{path}_preprocessed", combine_city_data=True, resample_freq="60T",
                                add_lockdown_info=True, clean_data=True, force_full_day=True)

    scraper = Scraper(["P1", "P2"], start_date=date(2017, 1, 1), end_date=date(2017, 1, 1), sensor_types=["sds011"],
                      preprocessor=preprocessor, location="Stuttgart_Germany")
    scraper.start()
