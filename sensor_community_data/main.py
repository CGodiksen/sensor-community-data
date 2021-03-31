import logging
import time
from datetime import date

from sensor_community_data.preprocessor import Preprocessor
from sensor_community_data.scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = f"../data/{int(time.time())}"

    preprocessor = Preprocessor(f"{path}_preprocessed")

    scraper = Scraper(["P1", "P2"], "sds011", start_date=date(2017, 1, 1), end_date=date(2017, 1, 3), preprocessor=preprocessor)
    scraper.start()
