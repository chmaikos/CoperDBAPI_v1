from datetime import datetime
from math import cos, radians

import pymongo
from flask import Flask, jsonify, request
from pymongo import DESCENDING

app = Flask(__name__)

myclient = pymongo.MongoClient("mongodb://mongodb:27017")
db = myclient["kafka_db"]
mycol_wave = db["waveData"]
mycol_wind = db["windData"]


def create_square(latitude, longitude, radius):
    earth_radius = 6371.0
    lat_rad = radians(latitude)
    radius_deg = radius / earth_radius
    min_latitude = latitude - radius_deg
    max_latitude = latitude + radius_deg
    min_longitude = longitude - (radius_deg / cos(lat_rad))
    max_longitude = longitude + (radius_deg / cos(lat_rad))
    return min_latitude, min_longitude, max_latitude, max_longitude


def search_data(start_date, end_date,
                min_longitude, max_longitude,
                min_latitude, max_latitude,
                database):
    query = {}
    sort_order = [("time", DESCENDING)]
    limit = 1
    result = database.find(query).sort(sort_order).limit(limit)
    date_format = "%Y-%m-%d %H:%M:%S"
    most_recent_date = [datetime.strptime(document['time'], date_format)
                        for document in result]
    if min(most_recent_date) > end_date or max(most_recent_date) < start_date:
        return {database.name: []}
    else:
        query = {
            "longitude": {"$gte": min_longitude, "$lte": max_longitude},
            "latitude": {"$gte": min_latitude, "$lte": max_latitude}
        }
        last_data = database.find(query)
        if not last_data:
            return {database.name: []}
        else:
            data_list = [doc for doc in last_data]
            for data in data_list:
                data.pop("_id", None)
            return {database.name: data_list}


@app.route('/data', methods=['GET'])
def get_data():
    latitude, longitude, radius = map(float, [request.args.get('latitude'),
                                              request.args.get('longitude'),
                                              request.args.get('radius')])
    lat_min, lon_min, lat_max, lon_max = create_square(latitude,
                                                       longitude,
                                                       radius)

    date_format = "%Y-%m-%d %H:%M:%S"
    date_min = datetime.strptime(request.args.get('dateMin').replace('T', ' '),
                                 date_format)
    date_max = datetime.strptime(request.args.get('dateMax').replace('T', ' '),
                                 date_format)
    info_return = [
        search_data(date_min, date_max,
                    lon_min, lon_max,
                    lat_min, lat_max,
                    mycol_wave),
        search_data(date_min, date_max,
                    lon_min, lon_max,
                    lat_min, lat_max,
                    mycol_wind)
    ]
    return jsonify(info_return)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
