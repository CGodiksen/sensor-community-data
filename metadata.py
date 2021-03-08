"""
Utility functions for gathering metadata about the data from the Sensor.Community archive.
"""
import os
import time
from collections import defaultdict
from datetime import date, timedelta

import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np

from preprocessor import Preprocessor
from scraper import Scraper


def plot_sensor_location_distribution(sensor_types, date, data_folder=None):
    """Plots a heatmap of how the sensors are distributed worldwide on the given date."""
    if not data_folder:
        data_folder = __location_data_scrape(sensor_types, date)

    # Go through each folder, count the number of files in the folder and add it to the count of the country.
    country_sensor_count = defaultdict(int)
    for folder in next(os.walk(data_folder))[1]:
        country = folder.split("_")[1]
        country_sensor_count[country] += len(os.listdir(f"{data_folder}/{folder}/"))

    # The query year does not matter, it is only used for getting a dataframe that can be added to.
    df = px.data.gapminder().query("year==2007")
    df["sensor_count"] = 0
    for i, row in df.iterrows():
        df.at[i, "sensor_count"] = country_sensor_count.get(row["country"])

    # Plot the modified dataframe in a choropleth map.
    fig = px.choropleth(df, locations="iso_alpha",
                        color=np.log10(df["sensor_count"]),
                        hover_name="country",
                        color_continuous_scale=px.colors.sequential.Viridis)

    fig.update_layout(coloraxis_colorbar=dict(
        title=dict(text="Sensor count", font=dict(size=16)),
        thicknessmode="pixels", thickness=60,
        yanchor="top", y=1,
        xanchor="right", x=1.03,
        ticks="outside",
        tickfont=dict(size=14),
        tickvals=np.log10([1000, 2000, 3000, 4000, 5000]),
        ticktext=["1000", "2000", "3000", "4000", "5000"],
    ))
    fig.show()


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

    start_date_str = dates[0].strftime("%Y-%m")
    end_date_str = dates[-1].strftime("%Y-%m")
    ax.set(ylabel='Sensor Count', title=f"Sensor count from {start_date_str} to {end_date_str}")
    plt.xticks(rotation=45)

    plt.show()


# Perform a data scrape from the given date that allows for sensor location analysis.
def __location_data_scrape(date, sensor_types):
    data_folder = f"data/{int(time.time())}_preprocessed"

    # Scrape and preprocess the data for the single day without combining city data.
    preprocessor = Preprocessor(data_folder)
    scraper = Scraper([], start_date=date, end_date=date, sensor_types=sensor_types, preprocessor=preprocessor)
    scraper.start()

    return data_folder


# plot_sensor_count(["sds011"], date(2017, 1, 1), date(2021, 3, 1))
plot_sensor_location_distribution(["sds011"], date(2021, 1, 1), "data/1615214611_preprocessed")
