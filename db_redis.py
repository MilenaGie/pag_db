import redis
import geojson
import numpy as np
from csv import reader
from shapely import wkt
from shapely.geometry import Point, Polygon
from base_analysis import read_shp


def import_stations(db, path):
    with open(path, encoding="utf8") as json_file:
        data = geojson.load(json_file)

    for feature in data["features"]:
        coor = feature["geometry"]["coordinates"]
        # transformer = Transformer.from_crs("EPSG:2180", "EPSG:4326")
        # geom = transformer.transform(coor[0], coor[1])
        # db.geoadd("stations", [geom[1], geom[0], codeSH])
        attributes = feature["properties"]
        codeSH = attributes["name"]
        geom = f"{str(coor[0])}, {str(coor[1])}"
        db.hset("stations", codeSH, geom)
    # print(db.geopos("stations", "149180090"))


def import_shp_data(db, path, geo_data_type):
    df = read_shp(path)
    for row in df.iterrows():
        data = list(row[1])
        geom = wkt.dumps(data[2])
        db.hset(geo_data_type, data[1], geom)


def get_string_to_geometry(geom):
    result = []
    shape = geom
    repl = ["POLYGON", "MULTI", "(", ")"]
    for r in repl:
        shape = shape.replace(r, "")
    shape = shape.split(",")
    for coor in shape:
        c = coor.split()
        result.append((float(c[0]), float(c[1])))
    return result


def import_IMGW_data(db, path):
    with open(path, encoding='utf-8-sig') as file:
        csv_reader = reader(file, delimiter=';')
        for row in csv_reader:
            codeSH, date, value = row[0], row[2].replace(" ", "."), float(row[3].replace(",", "."))
            db.hset(codeSH, date, value)


def join_codeSH_with_geometries(db, data_geo_type, geo_type_name):
    stations = db.hgetall("stations")
    geo = db.hget(data_geo_type, geo_type_name)
    poly = Polygon(get_string_to_geometry(geo))
    codes = []
    for codeSH in stations:
        coor = stations[codeSH].split(",")
        pt = Point([float(coor[0]), float(coor[1])])
        if pt.within(poly):
            codes.append(codeSH)
    return codes


def join_IMGW_data_with_geometry(db, data_geo_type):
    for geo_name in db.hgetall(data_geo_type):
        codesSH = join_codeSH_with_geometries(db, data_geo_type, geo_name)
        for code in codesSH:
            records = db.hgetall(code)
            if len(records) != 0:
                for full_date in records:
                    date, hour = full_date.split(".")
                    name = f"{geo_name}_{date}"
                    db.sadd("date_collections", date)
                    db.rpush(name, float(records[full_date]))


def calulate_statistics(db, data_geo_type, select=[]):
    for geo_name in db.hgetall(data_geo_type):
        if select == [] or geo_name in select:
            print(geo_name)
            dates = db.sort("date_collections", alpha=True)
            for date in dates:
                print(date)
                name = f"{geo_name}_{date}"
                data_for_day = db.lrange(name, 0, -1)
                data = np.array(data_for_day, dtype=np.float32)
                print(" Mean:", data.mean(), "\n Median:", np.median(data))


def main():
    pool = redis.ConnectionPool(host="127.0.0.1", port=6379, db=0, decode_responses=True)
    db = redis.Redis(connection_pool=pool)

    # geo_data_type - "woj"/ "pow"
    """
    # import_stations(db, "data/effacility.geojson")
    # import_shp_data(db, "data/woj.shp", "woj")
    # import_shp_data(db, "data/powiaty.shp", "pow")
    # import_IMGW_data(db, "data/B00305A_2023_09.csv")

    # join_IMGW_data_with_geometry(db, "woj")
    # join_IMGW_data_with_geometry(db, "pow")
    """

    calulate_statistics(db, "woj", ["podkarpackie", "śląskie"])
