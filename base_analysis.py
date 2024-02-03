import pandas as pd
from pandas import DataFrame
import geopandas as gpd
from geopandas import GeoDataFrame
import json
import numpy as np
import matplotlib.pyplot as plt


# B00020S   - Stan wody operacyjny
# B00050S   - Przepływ operacyjny
# B00014A   - Stan wody kontrolny
# B00101A   - Temperatura wody  (obserwator)
# B00305A   - Temperatura gruntu (czujnik)

def read_file_csv(path):
    column_names = ["codeSH", "paramSH", "fullDate", "value", "empty"]
    df = pd.read_csv(path, names=column_names, sep=";")
    df[["date", "fullHour"]] = df.fullDate.str.split(" ", expand=True)  # split fullDate into columns date and hour
    df[["year", "month", "day"]] = df.date.str.split("-", expand=True)
    df[["hour", "min"]] = df.fullHour.str.split(":", expand=True)
    df['value'] = df['value'].str.replace(",",'.')
    df = df.drop(columns=["empty", "paramSH", "fullDate", "date", "fullHour"])
    df = df.sort_values(["year", "month", "day", "hour", "min"])
    return df


def read_file_json(path):
    file_json = gpd.read_file(path)
    file_json.crs = 2180
    return file_json


def read_shp(path):
    df = gpd.read_file(path)
    data = df[["id", "name", "geometry"]]
    return data


def group_by_day(df):
    hours = df["hour"].apply(int).to_list()
    h1 = hours >= np.full_like(hours, 5)
    h2 = hours < np.full_like(hours, 22)
    h = h1 == h2
    # ^!! dla wartosci false i false bedzie true;
    #  jesli przedzial zostal dobrze zdefiniowany, to taki przypadek nie nastapi
    day_night = np.where(h, "day", "night")
    df["day_night"] = day_night
    df_by_time = df.groupby(["year", "month", "day", "day_night"])
    return df_by_time


# statystyki na kazdy dzien z uwzglednieniem pory dania
def get_statistic(df, trimmed_perc=5):
    df_by_time = group_by_day(df)
    mean = df_by_time["value"].mean()                                   # srednia
    median = df_by_time["value"].median()                               # mediana
    trim_mean = 0  # stats.trim_mean(df_by_time.value, trimmed_perc / 100)   # srednia obcinana
    return mean, median, trim_mean



def main():
    df = read_file_csv("data/B00305A_2023_09.csv")
    df_stations = read_file_json("data/effacility.geojson")
    df_woj = read_shp("data/woj.shp")
    df_pow = read_shp("data/powiaty.shp")
    #print(df_pow)
    # get_statistic(df)
    # print(df.groupby("codeSH").apply(lambda x: x))
    # print(df_stations)

    # df.plot()
    # plt.show()