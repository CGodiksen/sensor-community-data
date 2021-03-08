"""
Utility functions for gathering metadata about the data from the Sensor.Community archive.
"""
import matplotlib.pyplot as plt
from datetime import date, timedelta
from scraper import Scraper



def plot_sensor_location_distribution(sensor_types, date):
    """Plots a heatmap of how the given sensors are distributed worldwide on the given date."""
    pass


def plot_sensor_count(sensor_types, start_date, end_date):
    """Plots a line chart showing how many sensors there were each month between the given dates."""
    delta = end_date - start_date

    dates = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    dates = list(filter(lambda date: date.day == 1, dates))

    monthly_sensor_count = []
    for date in dates:
        scraper = Scraper([], start_date=date, end_date=date, sensor_types=sensor_types)
        monthly_sensor_count.append(len(scraper.get_file_urls(scraper.get_date_urls()[0])))

    fig, ax = plt.subplots()
    ax.plot(dates, monthly_sensor_count)

    start_date_str = start_date.strftime("%Y-%m")
    end_date_str = end_date.strftime("%Y-%m")
    ax.set(xlabel='Date', ylabel='Sensor Count', title=f"Monthly sensor count from {start_date_str} to {end_date_str}")

    plt.show()


plot_sensor_count(["sds011"], date(2017, 1, 1), date(2017, 12, 31))
