import os
from netCDF4 import Dataset
from netCDF4 import num2date
import numpy as np
import motuclient
import pandas as pd
import configparser
from datetime import datetime, timedelta
import pymongo
import uuid
from confluent_kafka import Producer
import json


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
    'bootstrap.servers': 'kafka1:9092'
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

output_dir = './'

# ==========================================================================
# Write data as a CSV table with 4 columns: time, latitude, longitude, value
# ==========================================================================
filename = os.path.join(output_dir, 'data/CMEMS_wave.csv')
print(f'Writing data in tabular form to {filename} (this may take some time)...')

times_grid, latitudes_grid, longitudes_grid = [x.flatten() for x in
                                               np.meshgrid(times, latitudes, longitudes, indexing='ij')]

print("times_grid", len(times_grid))
print("latitudes_grid", len(latitudes_grid))
print("longitudes_grid", len(longitudes_grid))

# Number of random IDs to generate
num_ids = len(times_grid)

# Generate a list of random IDs
random_ids = [str(uuid.uuid4()) for _ in range(num_ids)]

# print(random_ids)

df = pd.DataFrame({
    'id': random_ids,
    'time': [t.isoformat(sep=" ") for t in times_grid],
    'latitude': latitudes_grid,
    'longitude': longitudes_grid,
    'vhm0': vhm0[:].flatten(),
    'vmdr': vmdr[:].flatten(),
    'vtm10': vtm10[:].flatten(),
})

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


