from datetime import date
from scraper import Scraper

if __name__ == '__main__':
    test = Scraper(start_date=date(2016, 10, 8), end_date=date(2016, 10, 8), measurements=["P1", "P2"],
                   sensor_types=["sds011"])
