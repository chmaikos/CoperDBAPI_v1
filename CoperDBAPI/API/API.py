from flask import Flask, request, jsonify
from datetime import datetime
import pymongo
from pymongo import DESCENDING
import json

app = Flask(__name__)

list_csv = []

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = myclient["kafka_db"]
mycol = db["waveData"]


# Assuming you have a DataFrame called 'wind_data' with columns: 'date', 'longitude', 'latitude', and other data

# Function to search for rows with a specific date range and longitude/latitude range
def search_data(start_date, end_date, min_longitude, max_longitude, min_latitude, max_latitude, database):
    # Step 1: Check if the date range exists in the wind database
    print(type(database['date']))
    print(database['date'])

    # Query to get the most recent date
    query = {}

    # Sort the collection in descending order based on the 'date' field
    sort_order = [("date", DESCENDING)]

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

    if max(most_recent_date) < start_date:
        # If the date range does not exist, print (or return JSON) the message
        info = {"The range of dates doesn't exist, but the last information we have for this area is the following:"}

        # Query the database using the $gte and $lte operators
        query = {
            "longitude": {"$gte": min_longitude, "$lte": max_longitude},
            "latitude": {"$gte": min_latitude, "$lte": max_latitude}
        }

        print(query)
        # Retrieve the data that falls within the specified range
        last_data = database.find(query)

        print(last_data)

        # You can now loop through the 'last_data' cursor to access the documents returned by the query
        for document in last_data:
            print(document)

        # Sort the results by the date field in descending order
        most_recent_info = database.find(query).sort("date", -1)

        return dict(info) + dict(most_recent_info)
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
            return dict({"We don't have that information for this area."})
        else:

            # Convert the MongoDB cursor to a list of dictionaries
            data_list = [doc for doc in last_data]

            # Convert ObjectId to string in each dictionary
            for data in data_list:
                data.pop("id", None)
                data.pop("_id", None)

            # Convert the list of dictionaries to a JSON string with the 'records' orientation
            return_data = json.dumps(data_list, ensure_ascii=False, indent=4)

            # Convert the list of dictionaries to a JSON string with newline at the end of each item
            return_data = ',\n'.join([json.dumps(data, ensure_ascii=False) for data in data_list])

            # Wrap the result in square brackets to form a JSON array
            return_data = f"[{return_data}]"

            return return_data


@app.route('/data', methods=['GET'])
def get_data():
    latitude = float(request.args.get('latitude'))
    longitude = float(request.args.get('longitude'))
    radius = float(request.args.get('radius'))

    lat_min = latitude - radius
    lat_max = latitude + radius

    lon_min = longitude - radius
    lon_max = longitude + radius

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
    information = search_data(date_min, date_max, lon_min, lon_max, lat_min, lat_max, mycol)

    print(information)
    return information


@app.route('/listData', methods=['GET'])
def get_listdata():
    # Retrieve data from MongoDB
    data = list(mycol.find())

    # Convert the Cursor object to a list of dictionaries
    data_list = []
    for doc in data:
        doc['_id'] = str(doc['_id'])
        data_list.append(doc)

    # Convert data to JSON format and return as the response
    return jsonify(data_list)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
