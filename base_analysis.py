import pandas as pd
import geopandas as gpd
import numpy as np
from scipy import stats

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
    df['value'] = df['value'].str.replace(",", '.')
    df["value"] = pd.to_numeric(df["value"])
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
    day_night = np.where(h, "day", "night")
    df["day_night"] = day_night
    df_by_time = df.groupby(["year", "month", "day", "day_night"])
    return df_by_time


# statystyki na kazdy dzien z uwzglednieniem pory dania
def get_statistic(df, trimmed_perc=5):
    df_by_time = group_by_day(df)
    group_name, mean, median, trim_mean = [], [], [], []
    for record in list(df_by_time["value"]):
        group_name.append("-".join(record[0]))
        mean.append(record[1].mean())
        median.append(record[1].median())
        trim_mean.append(stats.trim_mean(record[1], trimmed_perc / 100))
    result = pd.DataFrame({"Name": group_name, "Mean": mean, "Median": median, "Trimmed_mean": trim_mean})
    print(result)


def point_df_to_gdf_with_geometry(df_points: pd.DataFrame, gdf_geometry: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    records_with_geometry_df = df_points.merge(gdf_geometry, left_on='codeSH', right_on='ifcid', how='left')
    records_with_geometry_gdf = gpd.GeoDataFrame(records_with_geometry_df,
                                                 geometry=records_with_geometry_df['geometry']).to_crs(epsg=2180)
    return records_with_geometry_gdf


def join_geometry_with_data(data, geo_data):
    data["codeSH"] = pd.to_numeric(data["codeSH"])
    geo_data["name_left"] = pd.to_numeric(geo_data["name_left"])
    merged = pd.merge(data, geo_data, how="left", left_on="codeSH", right_on="name_left")
    result = merged[["codeSH", "value", "year", "month", "day", "name_right"]]
    return result


def join_data_with_geometries(data, df_woj, df_pow, df_stations):
    stations_woj = df_stations.sjoin(df_woj, how="left")
    stations_pow = df_stations.sjoin(df_pow, how="left")
    # join geometry with data
    data_woj = join_geometry_with_data(data, stations_woj)
    data_pow = join_geometry_with_data(data, stations_pow)
    return data_woj, data_pow


def get_statistics_by_geometry(df_data, trimmed_perc=5, select=[]):
    for rec in list(df_data.groupby("name_right")):
        if select == [] or rec[0] in select:
            print(rec[0])
            data = rec[1].groupby(["year", "month", "day"])
            group_name, mean, median, trim_mean = [], [], [], []
            for record in list(data["value"]):
                group_name.append("-".join(record[0]))
                mean.append(record[1].mean())
                median.append(record[1].median())
                trim_mean.append(stats.trim_mean(record[1], trimmed_perc / 100))
            result = pd.DataFrame({"Name": group_name, "Mean": mean, "Median": median, "Trimmed_mean": trim_mean})
            print(result)


def main():
    df_data = read_file_csv("data/B00305A_2023_09.csv")
    df_stations = read_file_json("data/effacility.geojson")
    df_woj = read_shp("data/woj.shp")
    df_pow = read_shp("data/powiaty.shp")
    df_woj, df_pow = df_woj.to_crs(2180), df_pow.to_crs(2180)
    df_data_woj, df_data_pow = join_data_with_geometries(df_data, df_woj, df_pow, df_stations)

    # statystyka z podzialem na dzien/noc
    # get_statistic(df_data)

    # statystyka z podzialem na wojewodztwa lub powiaty
    # get_statistics_by_geometry(df_data_pow, select=["pszczyński", "poznański"])
    # get_statistics_by_geometry(df_data_woj, select=["mazowieckie", "opolskie"])
