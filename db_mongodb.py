import json

from geopandas import GeoDataFrame
from pandas import DataFrame
from pymongo import MongoClient
from pymongo.database import Collection, Database

from base_analysis import read_shp, read_file_json, read_file_csv, point_df_to_gdf_with_geometry


def record_gdf_to_insert_list(records_gdf: GeoDataFrame) -> list[dict]:
    records_json = records_gdf.to_json(to_wgs84=True)
    records_list = json.loads(records_json)['features']

    insert_list = []
    for record in records_list:
        if record['geometry'] is not None:
            temp = dict()
            temp['value'] = float(record['properties']['value'])
            temp['year'] = int(record['properties']['year'])
            temp['month'] = int(record['properties']['month'])
            temp['day'] = int(record['properties']['day'])
            temp['hour'] = int(record['properties']['hour'])
            temp['geometry'] = record['geometry']
            insert_list.append(temp)

    return insert_list


def area_gdf_to_insert_list(area_gdf: GeoDataFrame) -> list[dict]:
    area_json = area_gdf.to_json(to_wgs84=True)
    area_list = json.loads(area_json)['features']

    insert_list = []
    for record in area_list:
        temp = dict()
        temp['id'] = int(record['properties']['id'])
        temp['name'] = str(record['properties']['name'])
        temp['geometry'] = record['geometry']
        insert_list.append(temp)

    return insert_list


def insert_area_data(collection: Collection, gdf_area: GeoDataFrame):
    insert_list = area_gdf_to_insert_list(gdf_area)
    collection.insert_many(insert_list)
    collection.create_index(["geometry", "2dsphere"])


def insert_IMGW_data(collection: Collection, df_records: DataFrame, gdf_locations: GeoDataFrame):
    gdf_merged = point_df_to_gdf_with_geometry(df_records, gdf_locations)
    insert_list = record_gdf_to_insert_list(gdf_merged)
    collection.insert_many(insert_list)
    collection.create_index(["geometry", "2dsphere"])


def calculate_avg(collectionArea: Collection, collectionData: Collection):
    print(f"Calculating avg for all area files:")
    all_areas = collectionArea.count_documents({})

    for i, area in enumerate(collectionArea.find({})):
        try:
            cursor = collectionData.aggregate([
                {
                    "$match": {"geometry": {"$geoWithin": {"$geometry": area['geometry']}}}
                },
                {
                    "$group": {"_id": area["name"], "avg": {"$avg": "$value"}}
                }
            ])
            for x in cursor:
                collectionArea.update_one({"name": x["_id"]}, {"$set": {"mean": x["mean"]}})
            print(area['name'], f"({i + 1}/{all_areas})\t\t\t - calculation completed")
        except:
            collectionArea.update_one({"name": area["name"]}, {"$set": {"mean": 0}})
            print(area['name'], f"({i + 1}/{all_areas})\t\t\t - calculation error: wrong data")

    print("Completed")


def show_value(chosen_collection: Collection, name: str):
    for x in chosen_collection.find({'name': name}):
        print(x['name'], "=", x['avg'])


def main():
    # Importing data from files to dataframes and geodataframes
    '''
    df_IMGW = read_file_csv("data/B00305A_2023_09.csv")
    gdf_stations = read_file_json("data/effacility.geojson")
    gdf_woj = read_shp("data/woj.shp")
    gdf_pow = read_shp("data/powiaty.shp")
    '''

    # Connecting to MongoClient and setting up the database
    client: MongoClient = MongoClient("localhost", 27017)
    db: Database = client.db
    dataIMGW: Collection = db.dataIMGW
    dataWoj: Collection = db.dataWoj
    dataPow: Collection = db.dataPow

    # Insering data into appropriate tables
    '''
    insert_IMGW_data(dataIMGW, df_IMGW, gdf_stations)
    insert_area_data(dataWoj, gdf_woj)
    insert_area_data(dataPow, gdf_pow)
    '''

    # Calculating mean values and adding them to every file in selected collection
    '''
    calculate_avg(dataWoj, dataIMGW)
    calculate_avg(dataPow, dataIMGW)
    '''

    # Input and results
    show_value(dataWoj, 'mazowieckie')

    client.close()
