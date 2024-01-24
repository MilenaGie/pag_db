import redis
import json

pool = redis.ConnectionPool(host="127.0.0.1", port=6379, db=0)
db = redis.Redis(connection_pool=pool)


def read_file(path, encoding="UTF-8-sig"):
    object_vals = []
    with open(path, encoding=encoding) as data:
        for line in data.readlines():
            line = line.split(";")
            if len(line) > 1:
                if line[len(line) - 1] == "\n":
                    end_rec_id = len(line) - 1
                else:
                    end_rec_id = len(line)
                object_vals.append(line[0:end_rec_id])
    return object_vals


def import_data_to_redis(data):
    for i in range(len(data)):
        key = "id_" + str(i)

        date, full_hour = data[i][2].split(" ")
        year, month, day = date.split("-")
        hour, minute = full_hour.split(":")
        dict = {
            "codeSH": data[i][0],
            "param": data[i][1],
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
            "value": data[i][3]
        }
        db.hset(key, mapping=dict)


def count_coor(coor):
    print(coor)
    return float(coor[0]) + float(coor[1]) / 60 + float(coor[2]) / 3600


def import_stations(data):
    for rec in data:
        if rec[0] != "LP":
            # key = "id_" + rec[0]
            key = "sh" + rec[1]
            x = count_coor(rec[4])
            rec[4] = rec[4][0:2]
            y = count_coor(rec[5])
            dict = {
                # "codeSH": rec[1],
                "name": rec[2],
                "name2": rec[3],
                "X": x,
                "Y": y
            }
            db.hset(key, mapping=dict)


def main():
    data = read_file("data/B00014A_2022_12.csv")
    stations = read_file("data/kody_stacji.csv", "ANSI")

    # for rec in stations:
    #     print(rec)
    print(db.hgetall("sh152140090"))
    # import_data_to_redis(data)
    # print(db.hget("id_0", "codeSH"))
    # print(db.hgetall("id_0"))
