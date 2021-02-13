import logging
from datetime import date
from scraper import Scraper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    scraper = Scraper(start_date=date(2017, 1, 1), end_date=date(2017, 1, 1), measurements=["P1", "P2"],
                      sensor_types=["sds011"])
    scraper.start()
