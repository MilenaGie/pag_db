import matplotlib.pyplot as plt
from pymongo import MongoClient
import geopandas as gpd
import json
import pprint
from pymongo.database import Collection, Database
from base_analysis import read_shp, read_file_json, read_file_csv
from pandas import DataFrame
from geopandas import GeoDataFrame

def record_gdf_to_input_list(records_gdf: GeoDataFrame) -> list[dict]:
    records_json = records_gdf.to_json(to_wgs84=True)
    records_list = json.loads(records_json)['features']

    input_list = []
    for record in records_list:
        if record['geometry'] is not None:
            temp = dict()
            temp['value'] = float(record['properties']['value'])
            temp['year'] = int(record['properties']['year'])
            temp['month'] = int(record['properties']['month'])
            temp['day'] = int(record['properties']['day'])
            temp['hour'] = int(record['properties']['hour'])
            temp['geometry'] = record['geometry']
            input_list.append(temp)

    return input_list

def point_df_to_gdf_with_geometry(df_points: DataFrame, df_geometry: GeoDataFrame) -> GeoDataFrame:
    records_with_geometry_df = df_points.merge(df_geometry, left_on='codeSH', right_on='ifcid', how='left')
    records_with_geometry_gdf = gpd.GeoDataFrame(records_with_geometry_df, geometry=records_with_geometry_df['geometry']).to_crs(epsg=2180)
    return records_with_geometry_gdf

def main():
    # df = read_file_csv("data/B00305A_2023_09.csv")
    # df_stations = read_file_json("data/effacility.geojson")

    # df_woj = read_shp("data/woj.shp")
    # df_pow = read_shp("data/powiaty.shp")

    client: MongoClient = MongoClient("localhost", 27017)
    db: Database = client.temperature
    atemp: Collection = db.atemp
    # atemp.insert_many(record_gdf_to_input_list(point_df_to_gdf_with_geometry(df, df_stations))

    check = atemp.find({})
    for record in check:
        pprint.pprint(record)

    # print(Atemp.find_one())
    # ({})
    # for document in x:
    #     pprint.pprint(document)

    client.close()

