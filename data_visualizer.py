import pandas as pd
import matplotlib.pyplot as plt


class DataVisualizer:
    @staticmethod
    def get_line_chart(file_path):
        series = pd.read_csv(file_path, header=0, index_col=0, parse_dates=True, squeeze=True)
        series.plot()
        plt.show()


test = DataVisualizer()
test.get_line_chart("data/1613394996_preprocessed/Sofia-Bulgaria/Sofia-Bulgaria.csv")
