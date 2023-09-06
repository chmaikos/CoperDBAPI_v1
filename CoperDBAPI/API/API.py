from flask import Flask, request, jsonify
from datetime import datetime
import pymongo
from pymongo import DESCENDING
from math import radians, cos

app = Flask(__name__)

list_csv = []

myclient = pymongo.MongoClient("mongodb://mongodb:27017")
db = myclient["kafka_db"]
mycol_wave = db["waveData"]
mycol_wind = db["windData"]


def create_square(latitude, longitude, radius):
    # Earth's radius in kilometers
    earth_radius = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat_rad = radians(latitude)
    lon_rad = radians(longitude)

    # Convert radius from kilometers to degrees
    radius_deg = radius / earth_radius

    # Calculate the minimum and maximum latitude and longitude
    min_latitude = latitude - radius_deg
    max_latitude = latitude + radius_deg
    min_longitude = longitude - (radius_deg / cos(lat_rad))
    max_longitude = longitude + (radius_deg / cos(lat_rad))

    return min_latitude, min_longitude, max_latitude, max_longitude


def search_data_wave(start_date, end_date, min_longitude, max_longitude, min_latitude, max_latitude, database):

    # Step 1: Check if the date range exists in the wave database

    # Query to get the most recent date
    query = {}

    # Sort the collection in descending order based on the 'time' field
    sort_order = [("time", DESCENDING)]

    # Limit the query to only return one result
    limit = 1

    # Find the document with the most recent date
    result = database.find(query).sort(sort_order).limit(limit)

    date_format = "%Y-%m-%d %H:%M:%S"

    most_recent_date = []
    # Get the date from the result
    for document in result:
        most_recent_date.append(datetime.strptime(document['time'], date_format))
        print(most_recent_date)

    if min(most_recent_date) > end_date or max(most_recent_date) < start_date:
        return_data = {database.name: []}
        return dict(return_data)
    else:
        # Query the database using the $gte and $lte operators
        query = {
            "longitude": {"$gte": min_longitude, "$lte": max_longitude},
            "latitude": {"$gte": min_latitude, "$lte": max_latitude}
        }

        print(query)
        # Retrieve the data that falls within the specified range
        last_data = database.find(query)

        print(last_data)

        if not last_data:
            # If the longitude/latitude range does not exist, print (or return JSON) the message
            return_data = {database.name: []}
            return dict(return_data)
        else:

            # Convert the MongoDB cursor to a list of dictionaries
            data_list = [doc for doc in last_data]

            # Convert ObjectId to string in each dictionary
            for data in data_list:
                data.pop("_id", None)

            return_data = {database.name: data_list}

            return dict(return_data)


def search_data_wind(start_date, end_date, min_longitude, max_longitude, min_latitude, max_latitude, database):

    # Step 1: Check if the date range exists in the wave database

    # Query to get the most recent date
    query = {}

    # Sort the collection in descending order based on the 'time' field
    sort_order = [("time", DESCENDING)]

    # Limit the query to only return one result
    limit = 1

    # Find the document with the most recent date
    result = database.find(query).sort(sort_order).limit(limit)

    date_format = "%Y-%m-%d %H:%M:%S"

    most_recent_date = []
    # Get the date from the result
    for document in result:
        most_recent_date.append(datetime.strptime(document['time'], date_format))
        print(most_recent_date)

    if min(most_recent_date) > end_date or max(most_recent_date) < start_date:
        return_data = {database.name: []}
        return dict(return_data)
    else:
        # Query the database using the $gte and $lte operators
        query = {
            "longitude": {"$gte": min_longitude, "$lte": max_longitude},
            "latitude": {"$gte": min_latitude, "$lte": max_latitude}
        }

        print(query)
        # Retrieve the data that falls within the specified range
        last_data = database.find(query)

        print(last_data)

        if not last_data:
            # If the longitude/latitude range does not exist, print (or return JSON) the message
            return_data = {database.name: []}
            return dict(return_data)
        else:

            # Convert the MongoDB cursor to a list of dictionaries
            data_list = [doc for doc in last_data]

            # Convert ObjectId to string in each dictionary
            for data in data_list:
                data.pop("_id", None)

            return_data = {database.name: data_list}

            return dict(return_data)


@app.route('/data', methods=['GET'])
def get_data():

    info_return = []
    latitude = float(request.args.get('latitude'))
    longitude = float(request.args.get('longitude'))
    radius = float(request.args.get('radius'))

    lat_min, lon_min, lat_max, lon_max = create_square(latitude, longitude, radius)

    date_format = "%Y-%m-%d %H:%M:%S"

    date_min = request.args.get('dateMin')
    # Replace 'T' with a space
    date_min = date_min.replace('T', ' ')
    date_min = datetime.strptime(date_min, date_format)

    date_max = request.args.get('dateMax')
    # Replace 'T' with a space
    date_max = date_max.replace('T', ' ')
    date_max = datetime.strptime(date_max, date_format)

    # Example usage:
    info_return.append(search_data_wave(date_min, date_max, lon_min, lon_max, lat_min, lat_max, mycol_wave))
    info_return.append(search_data_wind(date_min, date_max, lon_min, lon_max, lat_min, lat_max, mycol_wind))

    return jsonify(info_return)


# @app.route('/listData', methods=['GET'])
# def get_listdata():
#     # Retrieve data from MongoDB
#     data = list(mycol.find())
#
#     # Convert the Cursor object to a list of dictionaries
#     data_list = []
#     for doc in data:
#         doc['_id'] = str(doc['_id'])
#         data_list.append(doc)
#
#     # Convert data to JSON format and return as the response
#     return jsonify(data_list)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
