from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
import socket
import csv
from datetime import datetime
import pymongo

app = Flask(__name__)

list_csv = []

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = myclient["kafka_db"]
mycol = db["aisData"]


@app.route('/data', methods=['GET'])
def get_data():
    # Retrieve data from MongoDB
    data = list(mycol.find())

    # Serialize the ObjectId to string
    for doc in data:
        doc['_id'] = str(doc['_id'])

    # Convert data to JSON format and return as the response
    return jsonify(data)


if __name__ == "__API__":
    app.run(host="0.0.0.0", port=5000)
