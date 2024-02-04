from geopandas import GeoDataFrame
from neo4j import GraphDatabase
from pandas import DataFrame

from base_analysis import read_shp, read_file_json, read_file_csv


def create_query_for_woj_row(ind: int, woj_id: int, woj_name: str):
    row_query = '(w' + str(ind) + ':voivodeship {' + f'id: {woj_id}, name: "{woj_name}"' + '})'
    return row_query


def create_query_for_pow_row(ind_w: int, ind_p: int, pow_id: int, pow_name: str):
    row_query = ',(p' + str(ind_p) + ':county {' + f'id: {pow_id}, name: "{pow_name}"' + '}),'
    row_query += f'(p{str(ind_p)}) - [: WITHIN] -> (w{str(ind_w)})'
    return row_query


def create_query_for_station_row(ind_p: int, ind_s: int, station_id: int, station_name: str):
    row_query = ',(s' + str(ind_s) + ':station {' + f'id: {station_id}, name: "{station_name}"' + '}),'
    row_query += f'(s{str(ind_s)}) - [: WITHIN] -> (p{str(ind_p)})'
    return row_query


def create_query_for_IMGW_row(name: str, r_codeSH: str, r_value: str, r_year: str, r_month: str, r_day: str,
                              r_hour: str):
    row_query = 'MATCH (r_station:station {id: ' + r_codeSH + '})'
    row_query += 'CREATE (:record {' + f'{name}: {r_value}, year: {r_year}, month: {r_month},day: {r_day}, hour: {r_hour}' + '})'
    row_query += '- [:FROM] -> (r_station);'
    return row_query


def insert_IMGW_data(session, df_records: DataFrame, name: str):
    print("Begining the insertion of IMGW data")
    for i, ind in enumerate(df_records.index):
        query = create_query_for_IMGW_row(name, str(df_records['codeSH'][ind]), str(float(df_records['value'][ind])),
                                          str(int(df_records['year'][ind])), str(int(df_records['month'][ind])),
                                          str(int(df_records['day'][ind])), str(int(df_records['hour'][ind])))
        print("Calculating addition for record: " + str(i) + "/" + str(len(df_records)))
        session.run(query)
        if i == 10000:
            break
    print("Insertion complete")


def calculate_avg(session, v_not_c: bool):
    query = "MATCH (r:record)-[:FROM]->(s:station)-[:WITHIN]->"
    if v_not_c:
        query += "(c:county)-[:WITHIN]->(target:voivodeship)"
    else:
        query += "(target:county)"
    query += 'WITH target, avg(r.temperature) as avg'
    query += " SET target.avgtemp = avg"
    session.run(query)


def show_value(session, name: str, v_not_c: bool):
    if v_not_c:
        query = 'MATCH (target:voivodeship {name: "' + name + '"}) '
    else:
        query = 'MATCH (target:county {name: "' + name + '"}) '
    query += 'RETURN target.name as Name, target.avgtemp as AverageTemperature'
    result = session.run(query)
    for r in result:
        print(r)


def insert_area_data(session, gdf_woj: GeoDataFrame, gdf_pow: GeoDataFrame, gdf_stations: GeoDataFrame):
    for ind_w in gdf_woj.index:
        # Setting up the query
        query = 'CREATE '
        # Adding to the query to create a node for every voivodeship
        query += create_query_for_woj_row(ind_w, gdf_woj['id'][ind_w], gdf_woj['name'][ind_w])

        # Finding all counties within said voivodeship
        gdf_pow_within_woj = gdf_pow[gdf_pow.within(gdf_woj['geometry'][ind_w])]
        for ind_p in gdf_pow_within_woj.index:
            # Adding to the query to create a node for every county and connect it to the said voivodeship
            query += create_query_for_pow_row(ind_w, ind_p, gdf_pow_within_woj['id'][ind_p],
                                              gdf_pow_within_woj['name'][ind_p])
            # Finding all stations within said county
            gdf_stations_within_pow = gdf_stations[gdf_stations.within(gdf_pow_within_woj['geometry'][ind_p])]
            for ind_s in gdf_stations_within_pow.index:
                # Adding to the query to create a node for every station and connect it to the said county
                query += create_query_for_station_row(ind_p, ind_s, gdf_stations_within_pow['ifcid'][ind_s],
                                                      gdf_stations_within_pow['name'][ind_s])
        # Finalizing the query and executing it
        query += ';'
        session.run(query)


def main():
    # Importing data from files to dataframes and geodataframes
    df_IMGW = read_file_csv("data/B00305A_2023_09.csv")
    gdf_stations = read_file_json("data/effacility.geojson")
    gdf_woj = read_shp("data/woj.shp")
    gdf_pow = read_shp("data/powiaty.shp")

    # Connecting to the database
    user, password = "neo4j", "neo4j_password"
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=(user, password))
    session = driver.session()

    # Inserting data into the database
    insert_area_data(session, gdf_woj, gdf_pow, gdf_stations)
    insert_IMGW_data(session, df_IMGW, "temperature")

    # Calculating avg values for all counties and voivodeships
    calculate_avg(session, 0)
    calculate_avg(session, 1)

    # Printing chosen data
    show_value(session, 'mazowieckie', 1)
    show_value(session, 'kartuski', 0)
    session.close()
