import os
from netCDF4 import Dataset
from netCDF4 import num2date
import numpy as np
import pandas as pd
import cdsapi
import configparser
from datetime import datetime, timedelta
import time
import numpy.ma as ma
from math import pi
from confluent_kafka import Producer
import json
import pymongo
from math import radians, cos


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


# Create the Kafka producer
producer = Producer({
    'bootstrap.servers': 'kafka1:29092'
})

# Get the list of topics
topic_metadata = producer.list_topics()
topic_list = topic_metadata.topics

# Print the topics
for topic in topic_list:
    print("-----------------------------------------------------------------------------------------", topic)

topic = 'wind_topic'
# data = {'message': 'Hello, Kafka!'}


def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()}')


myclient = pymongo.MongoClient("mongodb://mongodb:27017")
db = myclient["kafka_db"]
mycol = db["windData"]

while True:
    config = configparser.ConfigParser()
    config.read('config.conf')

    lon = float(config.get('Default', 'longitude'))
    lat = float(config.get('Default', 'latitude'))
    rad = float(config.get('Default', 'radius'))

    # longitude_min = lon - rad
    # longitude_max = lon + rad
    #
    # latitude_min = lat - rad
    # latitude_max = lat + rad

    latitude_min, longitude_min, latitude_max, longitude_max = create_square(lat, lon, rad)

    # while True:
    # Get the current time
    current_time = datetime.now()
    current_time = current_time - timedelta(days=6)

    # Extract month, day, and time components
    month_current = current_time.strftime("%m")
    day_current = current_time.strftime("%d")  # Day of the month (zero-padded)
    time_current = current_time.strftime("%H:%M")  # Time in HH:MM format

    # Retrieve Wind data from Copernicus C3S and store as netCDF4 file using CDS API
    c = cdsapi.Client()

    windData = 'data/ERA5_Wind3H.nc'
    # windData_Feb_2022 = 'data/ERA5_Wind3H.nc'

    dataList = [
        {'fileLocation': windData, 'month': month_current, 'day': day_current}]
    print("month", dataList[0]["month"])
    print("day", dataList[0]["day"])
    for item in dataList:
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind'],
                'year': '2023',
                'month': item['month'],
                'day': item['day'],
                'time': [
                    '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00', '21:00', '22:00', '23:00',
                ],
                'area': [latitude_max, longitude_min, latitude_min, longitude_max],
                'format': 'netcdf',
            },
            item['fileLocation'])

    windData = Dataset('data/ERA5_Wind3H.nc', 'r+')

    windData_BL = windData
    # Extract variables for combined wind/wave dataset
    u10 = windData_BL.variables['u10']
    v10 = windData_BL.variables['v10']

    # Define the new variable --> wind speed
    wind_speed = windData_BL.createVariable('wind_speed', np.int16, ('time', 'latitude', 'longitude'))
    wind_speed.units = 'm s**-1'
    wind_speed.long_name = '10 metre wind speed'

    # Calculate the square of u10, v10 and write to u10, v10 variable in netCDF4 file
    u10sq = u10[:] ** 2
    v10sq = v10[:] ** 2

    # Calculate wind speed from u,v components
    wind_speed = ma.sqrt(u10sq + v10sq)
    wind_speed[:]

    # Define the new variable --> wind direction
    wind_dir = windData_BL.createVariable('wind_dir', np.int16, ('time', 'latitude', 'longitude'))
    wind_dir.units = 'deg'
    wind_dir.long_name = '10 metre wind direction'

    # Calculate wind direction from u,v components
    wind_dir = (270 - np.arctan2(v10[:], u10[:]) * 180 / pi) % 360
    wind_dir[:]

    # convert time dimension to string
    nctime = windData_BL.variables['time'][:]  # get values
    t_unit = windData_BL.variables['time'].units  # get unit  "days since 1950-01-01T00:00:00Z"
    t_cal = windData_BL.variables['time'].calendar
    tvalue = num2date(nctime, units=t_unit, calendar=t_cal)
    str_time = [i.strftime("%Y-%m-%d %H:%M:%S") for i in tvalue]  # to display dates as string


    # Get dimensions assuming 3D: time, latitude, longitude
    time_dim, lat_dim, lon_dim = u10.get_dims()
    time_var = windData_BL.variables[time_dim.name]
    times = num2date(time_var[:], time_var.units)
    latitudes = windData_BL.variables[lat_dim.name][:]
    longitudes = windData_BL.variables[lon_dim.name][:]

    times_grid, latitudes_grid, longitudes_grid = [
        x.flatten() for x in np.meshgrid(times, latitudes, longitudes, indexing='ij')]

    df = pd.DataFrame({
        'time': [t.isoformat(sep=" ") for t in times_grid],
        'latitude': latitudes_grid,
        'longitude': longitudes_grid,
        'u10': u10[:].flatten(),
        'v10': v10[:].flatten(),
        'speed': wind_speed[:].flatten(),
        'direction': wind_dir[:].flatten()
    })

    print(df)

    # Find null values in 'Feature1'
    null_values = df['u10'].isnull()

    # Filter the DataFrame to show only rows with null values in 'Feature1'
    rows_with_null = df[null_values]

    # Print the rows with null values
    print(rows_with_null)

    # Drop rows with null values in 'u10'
    df = df.dropna(subset=['u10'])

    print(df)

    # Convert DataFrame to a list of dictionaries (JSON-like documents)
    data = df.to_dict(orient='records')

    # Insert the data into MongoDB
    mycol.insert_many(data)

    # Optional: Close the MongoDB connection
    myclient.close()

    for index, row in df.iterrows():

        data_topic = row.to_dict()
        # # Convert dictionary to JSON bytes
        value = json.dumps(data_topic).encode('utf-8')

        producer.produce(topic=topic, value=value, callback=delivery_report)

        producer.flush()

    file_path = 'data/ERA5_Wind3H.nc'

    try:
        os.remove(file_path)
        print("File deleted successfully.")
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print(f"Error: {e}")

    # Sleep for 1 day
    time.sleep(24 * 60 * 60)
