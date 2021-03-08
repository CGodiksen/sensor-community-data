"""
Utility functions for gathering metadata about the data from the Sensor.Community archive.
"""
import matplotlib
import numpy as np
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


plot_sensor_count(["sds011"], date(2020, 1, 1), date(2020, 12, 31))
