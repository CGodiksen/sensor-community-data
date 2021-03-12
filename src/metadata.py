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
import pycountry_convert as pc

from src.preprocessor import Preprocessor
from src.scraper import Scraper


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
        title=dict(text="Sensor count", font=dict(size=18)),
        thicknessmode="pixels", thickness=60,
        yanchor="top", y=1,
        xanchor="right", x=1.03,
        ticks="outside",
        tickfont=dict(size=16),
        tickvals=np.log10([10, 25, 50, 100, 250, 500, 1000, 2500, 5000]),
        ticktext=["10", "25", "50", "100", "250", "500", "1000", "2500", "5000"],
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

        month_sensor_count = len(scraper.get_file_urls(scraper.get_date_urls()[0]))
        monthly_sensor_count.append(month_sensor_count)
        print(f"{date.strftime('%Y-%m')}: {month_sensor_count}")

    fig, ax = plt.subplots()
    ax.plot(dates, monthly_sensor_count)

    ax.set(ylabel='Sensor Count')
    plt.xticks(rotation=45)

    plt.savefig("data/sensor_count.pdf")
    plt.show()


def sensor_location_continent_info(sensor_types, date, data_folder=None):
    """Print information about how the sensors are distributed into continents."""
    if not data_folder:
        data_folder = __location_data_scrape(sensor_types, date)

    continent_sensor_count = defaultdict(int)
    for folder in next(os.walk(data_folder))[1]:
        country = folder.split("_")[1]
        try:
            country_alpha2 = pc.country_name_to_country_alpha2(country)

            continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
            continent_name = pc.convert_continent_code_to_continent_name(continent_code)

            continent_sensor_count[continent_name] += len(os.listdir(f"{data_folder}/{folder}/"))
        except KeyError:
            print(f"Could not find continent for {country}")

    print(continent_sensor_count)

    total = sum(continent_sensor_count.values())
    for continent, sensor_count in continent_sensor_count.items():
        print(f"{continent}: {(sensor_count / total) * 100}% of total")


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
# sensor_location_continent_info(["sds011"], date(2021, 1, 1), "data/1615214611_preprocessed")
