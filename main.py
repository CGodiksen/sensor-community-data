from datetime import date
from scraper import Scraper

if __name__ == '__main__':
    test = Scraper(start_date=date(2017, 3, 3), end_date=date(2017, 3, 3), measurements=["P1", "P2"],
                   sensor_types=["sds011"])
