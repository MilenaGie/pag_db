from geopandas import GeoDataFrame
from neo4j import GraphDatabase
from pandas import DataFrame

from base_analysis import read_shp, read_file_json, read_file_csv, point_df_to_gdf_with_geometry


def create_query_for_IMGW_row(r_codeSH: str, r_value: str, r_year: str, r_month: str, r_day: str, r_hour: str):
    row_query = (
                'CREATE (:pomiarIMGW {' + f'codeSH: toFloat({r_codeSH}), value: toFloat({r_value}), year: toFloat({r_year}), month: toFloat({r_month}),day: toFloat({r_day}), hour: toFloat({r_hour})' + '});')
    return row_query


def insert_IMGW_data(df_points: DataFrame, gdf_geometry: GeoDataFrame):
    gdf = point_df_to_gdf_with_geometry(df_points, gdf_geometry)
    print(gdf[['lon', 'lat']])
    for ind in gdf.index:
        if gdf['geometry'][ind].is_valid():
            print(create_query_for_IMGW_row(str(gdf['value'][ind]), str(gdf['year'][ind]), str(gdf['month'][ind]),
                                            str(gdf['day'][ind]), str(gdf['hour'][ind]), str(gdf['lat'][ind]),
                                            str(gdf['lon'][ind])))
        exit()


def create_query_for_woj_row(ind: int, woj_id: int, woj_name: str):
    row_query = '(w' + str(ind) + ':woj {' + f'woj_id: {woj_id}, woj_name: "{woj_name}"' + '})'
    return row_query


def create_query_for_pow_row(ind_w: int, ind_p: int, pow_id: int, pow_name: str):
    row_query = ',(p' + str(ind_p) + ':pow {' + f'pow_id: {pow_id}, pow_name: "{pow_name}"' + '}),'
    row_query += f'(p{str(ind_p)}) - [: WITHIN] -> (w{str(ind_w)})'
    return row_query


def create_query_for_station_row(ind_s: int, ind_p: int, pow_id: int, pow_name: str):
    row_query = ',(s' + str(ind_s) + ':station {' + f'pow_id: {pow_id}, pow_name: "{pow_name}"' + '}),'
    row_query += f'(s{str(ind_s)}) - [: WITHIN] -> (p{str(ind_p)})'
    return row_query


def insert_data(session, gdf_woj: GeoDataFrame, gdf_pow: GeoDataFrame, gdf_stations: GeoDataFrame,
                df_records: DataFrame):
    for ind_w in gdf_woj.index:
        query = 'CREATE '
        query += create_query_for_woj_row(ind_w, gdf_woj['id'][ind_w], gdf_woj['name'][ind_w])
        gdf_pow_within_woj = gdf_pow[gdf_pow.within(gdf_woj['geometry'][ind_w])]
        for ind_p in gdf_pow_within_woj.index:
            query += create_query_for_pow_row(ind_w, ind_p, gdf_pow_within_woj['id'][ind_p],
                                              gdf_pow_within_woj['name'][ind_p])
            gdf_stations_within_pow = gdf_stations[gdf_stations.within(gdf_pow_within_woj['geometry'][ind_p])]
            for ind_s in gdf_stations_within_pow.index:
                print(gdf_stations_within_pow['ifcid'][ind_s])
                df_records_from_station = df_records[df_records['codeSH'] == gdf_stations_within_pow['ifcid'][ind_s]]
                for ind_r in df_records_from_station.index:
                    print(df_records_from_station['codeSH'][ind_r])
            exit()
            # query += create_query_for_station_row()

        query += ';'
        session.run(query)


def main():
    # Importing data from files to dataframes and geodataframes
    df_IMGW = read_file_csv("data/B00305A_2023_09.csv")
    gdf_stations = read_file_json("data/effacility.geojson")
    gdf_woj = read_shp("data/woj.shp")
    gdf_pow = read_shp("data/powiaty.shp")

    user, password = "neo4j", "neo4j_password"
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=(user, password))
    session = driver.session()
    # query = "game"
    # session.run(query)
    # print(df_IMGW.columns)
    # print(gdf_pow.columns)
    # print(gdf_woj.columns)
    insert_data(session, gdf_woj, gdf_pow, gdf_stations, df_IMGW)
    # insert_IMGW_data(df_IMGW, gdf_stations)

    session.close()
