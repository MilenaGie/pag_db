from neo4j import GraphDatabase
from base_analysis import read_shp,read_file_json,read_file_csv,point_df_to_gdf_with_geometry

def create_query_for_IMGW_row():
    x = 0

def create_query_for_area_row():
    x = 0

def insert_IMGW_data():
    x = 0

def insert_area_data(s):
    y = 0

def main():
    # Importing data from files to dataframes and geodataframes
    # df_IMGW = read_file_csv("data/B00305A_2023_09.csv")
    # gdf_stations = read_file_json("data/effacility.geojson")
    # gdf_woj = read_shp("data/woj.shp")
    # gdf_pow = read_shp("data/powiaty.shp")

    user, password = "neo4j", "neo4j_password"
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=(user,password))
    session = driver.session()
    query = "game"
    session.run(query)

    session.close()
