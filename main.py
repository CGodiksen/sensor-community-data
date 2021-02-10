import logging
from datetime import date
from scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    test = Scraper(start_date=date(2017, 10, 10), end_date=date(2017, 10, 10), measurements=["P1", "P2"],
                   sensor_types=["sds011"])
