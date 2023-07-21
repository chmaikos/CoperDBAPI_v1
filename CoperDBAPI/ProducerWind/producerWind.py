import os
from netCDF4 import Dataset
from netCDF4 import num2date
import numpy as np
import pandas as pd
import cdsapi
import configparser
from datetime import datetime, timedelta
import pymongo
import uuid
import numpy.ma as ma
from math import pi
from confluent_kafka import Producer
import json

# Create the Kafka producer
producer = Producer({
    'bootstrap.servers': 'kafka1:9092'
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

myclient = pymongo.MongoClient("mongodb://host.docker.internal:27017")
db = myclient["kafka_db"]
mycol = db["windData"]

config = configparser.ConfigParser()
config.read('config.conf')

lon = float(config.get('Default', 'longitude'))
lat = float(config.get('Default', 'latitude'))
rad = float(config.get('Default', 'radius'))

longitude_min = lon - rad
longitude_max = lon + rad

latitude_min = lat - rad
latitude_max = lat + rad

# while True:
# Get the current time
current_time = datetime.now()
current_time = current_time - timedelta(days=10)
# Subtract 2 hours from the current time
before_3_hours_time_date = current_time - timedelta(hours=3)

# Add one second to the before_3_hours_time_date to avoid duplicates
before_3_hours_time_date = before_3_hours_time_date + timedelta(seconds=1)

# Round the time to the nearest hour
rounded_time_3hours = before_3_hours_time_date.replace(minute=0)

# Extract month, day, and time components
month_3hours = before_3_hours_time_date.strftime("%m")
day_3hours = before_3_hours_time_date.strftime("%d")  # Day of the month (zero-padded)
time_3hours = rounded_time_3hours.strftime("%H:%M")  # Time in HH:MM format

before_5_hours_time_date = current_time - timedelta(hours=5)
# before_5_hours_time_date = before_5_hours_time_date + timedelta(seconds=1)

# Round the time to the nearest hour
rounded_time_5hours = before_5_hours_time_date.replace(second=0, microsecond=0) + timedelta(minutes=30)
rounded_time_5hours = rounded_time_5hours.replace(minute=0)
# # Round the time to the nearest hour
# rounded_time_5hours = before_5_hours_time_date.replace(minute=0)

# Extract month, day, and time components
month_5hours = before_5_hours_time_date.strftime("%m")
day_5hours = before_5_hours_time_date.strftime("%d")  # Day of the month (zero-padded)
time_5hours = rounded_time_5hours.strftime("%H:%M")  # Time in HH:MM format

# Retrieve Wind data from Copernicus C3S and store as netCDF4 file using CDS API
c = cdsapi.Client()

windData_Jan_2022 = 'data/ERA5_Wind3H.nc'
# windData_Feb_2022 = 'data/ERA5_Wind3H.nc'

dataList = [
    {'fileLocation': windData_Jan_2022, 'month': month_3hours, 'day': day_3hours, 'time': [time_5hours, time_3hours]}]
print("time", dataList[0]['time'])
print("day", dataList[0]['day'])
print("month", dataList[0]['month'])

for item in dataList:
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind'],
            'year': '2023',
            'month': item['month'],
            'day': item['day'],
            'time': dataList[0]['time'],
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
print("--------------time---------------", )
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

output_dir = './'

# ==========================================================================
# Write data as a CSV table with 4 columns: time, latitude, longitude, value
# ==========================================================================
filename = os.path.join(output_dir, 'data/ERA5_wind.csv')
print(f'Writing data in tabular form to {filename} (this may take some time)...')

times_grid, latitudes_grid, longitudes_grid = [
    x.flatten() for x in np.meshgrid(times, latitudes, longitudes, indexing='ij')]

print("times_grid", len(times_grid))
print("latitudes_grid", len(latitudes_grid))
print("longitudes_grid", len(longitudes_grid))

# Number of random IDs to generate
num_ids = len(times_grid)

# Generate a list of random IDs
random_ids = [str(uuid.uuid4()) for _ in range(num_ids)]

df = pd.DataFrame({
    'id': random_ids,
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

df.to_csv(filename, index=False)
print('Done')


