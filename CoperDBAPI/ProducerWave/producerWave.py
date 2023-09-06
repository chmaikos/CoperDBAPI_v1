import os
from netCDF4 import Dataset
from netCDF4 import num2date
import numpy as np
import motuclient
import pandas as pd
import configparser
from datetime import datetime, timedelta
import pymongo
import time
from confluent_kafka import Producer
import json
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


# Procedure to donwload Wave data from Copernicus Marine Service (CMEMS)
class MotuOptions:
    def __init__(self, attrs: dict):
        super(MotuOptions, self).__setattr__("attrs", attrs)

    def __setattr__(self, k, v):
        self.attrs[k] = v

    def __getattr__(self, k):
        try:
            return self.attrs[k]
        except KeyError:
            return None


def motu_option_parser(script_template, usr, pwd, output_filename):
    dictionary = dict(
        [e.strip().partition(" ")[::2] for e in script_template.split('--')])
    dictionary['variable'] = [value for (var, value) in
                              [e.strip().partition(" ")[::2] for e in script_template.split('--')] if var == 'variable']
    for k, v in list(dictionary.items()):
        if v == '<OUTPUT_DIRECTORY>':
            dictionary[k] = '.'
        if v == '<OUTPUT_FILENAME>':
            dictionary[k] = output_filename
        if v == '<USERNAME>':
            dictionary[k] = usr
        if v == '<PASSWORD>':
            dictionary[k] = pwd
        if k in ['longitude-min', 'longitude-max', 'latitude-min',
                 'latitude-max', 'depth-min', 'depth-max']:
            dictionary[k] = float(v)
        if k in ['date-min', 'date-max']:
            dictionary[k] = v[1:-1]
        dictionary[k.replace('-', '_')] = dictionary.pop(k)
    dictionary.pop('python')
    dictionary['auth_mode'] = 'cas'
    return dictionary


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

topic = 'wave_topic'
# data = {'message': 'Hello, Kafka!'}


def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()}')


myclient = pymongo.MongoClient("mongodb://host.docker.internal:27017")
db = myclient["kafka_db"]
mycol = db["waveData"]

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

    # Get the current time
    current_time = datetime.now()

    # Subtract 2 hours from the current time
    before_3_hours_time_date = current_time - timedelta(hours=3)

    # Add one second to the before_3_hours_time_date to avoid duplicates
    before_3_hours_time_date = before_3_hours_time_date + timedelta(seconds=1)

    # In order to download data you have to sign up/login to CMEMS -- provide the credentials
    # USERNAME = input('Enter your username: ')
    # PASSWORD = getpass.getpass('Enter your password: ')
    USERNAME = 'mmini1'
    PASSWORD = 'Artemis2000'

    '----------------------------------------------Wave Dataset-------------------------------------------------------'

    # Provide output directory/filenames
    OUTPUT_FILENAME = 'data/CMEMS_Wave3H.nc'

    # Change the variables according to the desired dataset
    script_template = f'python -m motuclient \
        --motu https://nrt.cmems-du.eu/motu-web/Motu \
        --service-id GLOBAL_ANALYSISFORECAST_WAV_001_027-TDS \
        --product-id cmems_mod_glo_wav_anfc_0.083deg_PT3H-i \
        --longitude-min {longitude_min} --longitude-max {longitude_max} \
        --latitude-min {latitude_min} --latitude-max {latitude_max} \
        --date-min "' + str(before_3_hours_time_date) + '" --date-max "' + str(current_time) + '" \
        --variable VHM0 --variable VMDR --variable VTM10 \
        --out-dir <OUTPUT_DIRECTORY> --out-name <OUTPUT_FILENAME> \
        --user <USERNAME> --pwd <PASSWORD>'

    print(script_template)

    data_request_options_dict_automated = motu_option_parser(script_template, USERNAME, PASSWORD, OUTPUT_FILENAME)

    # Motu API executes the downloads
    motuclient.motu_api.execute_request(MotuOptions(data_request_options_dict_automated))

    waveData = Dataset('data/CMEMS_Wave3H.nc', 'r+')

    waveData.set_auto_mask(True)

    # Extract variables for wave dataset
    vhm0 = waveData.variables['VHM0']
    vmdr = waveData.variables['VMDR']
    vtm10 = waveData.variables['VTM10']

    # Get dimensions assuming 3D: time, latitude, longitude
    time_dim, lat_dim, lon_dim = vhm0.get_dims()
    time_var = waveData.variables[time_dim.name]
    times = num2date(time_var[:], time_var.units)
    latitudes = waveData.variables[lat_dim.name][:]
    longitudes = waveData.variables[lon_dim.name][:]

    times_grid, latitudes_grid, longitudes_grid = [x.flatten() for x in
                                                   np.meshgrid(times, latitudes, longitudes, indexing='ij')]

    df = pd.DataFrame({
        'time': [t.isoformat(sep=" ") for t in times_grid],
        'latitude': latitudes_grid,
        'longitude': longitudes_grid,
        'vhm0': vhm0[:].flatten(),
        'vmdr': vmdr[:].flatten(),
        'vtm10': vtm10[:].flatten(),
    })

    print(df)

    # Find null values in 'Feature1'
    null_values = df['vhm0'].isnull()

    # Filter the DataFrame to show only rows with null values in 'Feature1'
    rows_with_null = df[null_values]

    # Print the rows with null values
    print(rows_with_null)

    # Drop rows with null values in 'vhm0'
    df = df.dropna(subset=['vhm0'])

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

    file_path = 'data/CMEMS_Wave3H.nc'

    try:
        os.remove(file_path)
        print("File deleted successfully.")
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print(f"Error: {e}")

    # Sleep for 3 hours
    time.sleep(3 * 3600)



