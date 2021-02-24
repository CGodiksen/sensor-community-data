import pandas as pd
from hampel import hampel
import matplotlib.pyplot as plt
from pathlib import Path


class DataVisualizer:
    @staticmethod
    def get_line_chart(folder_path):
        dataframes = []
        for csv_file in Path(folder_path).rglob("*.csv"):
            dataframes.append(pd.read_csv(csv_file, header=0, index_col=0, parse_dates=True, squeeze=True))

        series = pd.concat(dataframes)
        series_1 = series[["P1"]]

        series_cleaned = hampel(series_1.squeeze(), window_size=20, n=3)
        series_cleaned.plot()
        plt.show()


test = DataVisualizer()
test.get_line_chart("data/1614173522_preprocessed/Stuttgart_Germany")
