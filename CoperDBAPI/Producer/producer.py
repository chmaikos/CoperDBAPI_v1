from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import pymongo
import os
import cdsapi
from netCDF4 import Dataset
from netCDF4 import num2date
import numpy as np
import numpy.ma as ma
import xarray as xr
import motuclient
import getpass
from datetime import datetime, time
from dask.base import tokenize
import pandas as pd
from math import pi
import configparser
# importing datetime module for now()
from datetime import datetime, timedelta


def parse_datetime(datetime_str):
    return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")


myclient = pymongo.MongoClient("mongodb://root:example@localhost:27050/?authMechanism=DEFAULT")
db = myclient["kafka_db"]
mycol = db["aisData"]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
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


config = configparser.ConfigParser()
config.read('config.conf')

lon = float(config.get('Default', 'longitude'))
lat = float(config.get('Default', 'latitude'))
rad = float(config.get('Default', 'radius'))

longitude_min = lon - rad
longitude_max = lon + rad

latitude_min = lat - rad
latitude_max = lat + rad

# Get the current time
current_time = datetime.now()
current_time = current_time - timedelta(days=10)
current_time = current_time.replace(minute=0, second=0, microsecond=0)

# Subtract 2 hours from the current time
before_3_hours_time_date = current_time - timedelta(hours=3)

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

# Provide output directory/filenames
OUTPUT_FILENAME = 'data/CMEMS_Wave3H.nc'


# Change the variables according to the desired dataset
script_template = f'python -m motuclient \
--motu https://nrt.cmems-du.eu/motu-web/Motu \
--service-id GLOBAL_ANALYSISFORECAST_WAV_001_027-TDS \
--product-id cmems_mod_glo_wav_anfc_0.083deg_PT3H-i \
--longitude-min {longitude_min} --longitude-max {longitude_max} \
--latitude-min {latitude_min} --latitude-max {latitude_max} \
--date-min "'+str(before_5_hours_time_date)+'" --date-max "'+str(before_3_hours_time_date)+'" \
--variable VHM0 --variable VMDR --variable VTM10 \
--out-dir <OUTPUT_DIRECTORY> --out-name <OUTPUT_FILENAME> \
--user <USERNAME> --pwd <PASSWORD>'

print(script_template)

data_request_options_dict_automated_Jan_Feb = motu_option_parser(script_template, USERNAME, PASSWORD,
                                                                 OUTPUT_FILENAME)

# Motu API executes the downloads
motuclient.motu_api.execute_request(MotuOptions(data_request_options_dict_automated_Jan_Feb))

# # Combine Wave datasets into one
# waveDs = xr.open_mfdataset([OUTPUT_FILENAME], combine='nested', concat_dim='time')
#
# # Save the wave combined dataset to a new NetCDF4 file
# waveDs.to_netcdf('data/CMEMS_Wave3H.nc')

# Retrieve Wind data from Copernicus C3S and store as netCDF4 file using CDS API
c = cdsapi.Client()

windData_Jan_2022 = 'data/ERA5_Wind3H.nc'
# windData_Feb_2022 = 'data/ERA5_Wind3H.nc'

dataList = [{'fileLocation': windData_Jan_2022, 'month': month_3hours, 'day': day_3hours, 'time': [time_5hours, time_3hours]}]
print("time", dataList[0]['time'])
print("day", dataList[0]['day'])
print("month", dataList[0]['month'])

# 43.173113, 27.917765 50km-->radius
for item in dataList:
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind'],
            'year': '2023',
            'month': item['month'],
            'day': item['day'],
            'time': ['09:00'],
            'area': [45.95, -7.12, 30.15, 37.25],
            'format': 'netcdf',
        },
        item['fileLocation'])

# # Combine Wind datasets into one
# windDs = xr.open_mfdataset([windData_Jan_2022, windData_Feb_2022], combine='nested',
#                            concat_dim='time')
#
# # Save the wave combined dataset to a new NetCDF4 file
# windDs.to_netcdf('data/ERA5_Wind3H_Jan-Mar_2022.nc')

windData = Dataset('data/ERA5_Wind3H.nc', 'r+')
waveData = Dataset('data/CMEMS_Wave3H.nc', 'r+')

# Run remap.py file for bilinear and conservative interpolation datasets
import remap

windData_BL = Dataset('data/ERA5_Wind3H_remapBL.nc', 'r+')

waveData.set_auto_mask(True)

# Extract variables for combined wind/wave dataset
vhm0 = waveData.variables['VHM0']
vmdr = waveData.variables['VMDR']
vtm10 = waveData.variables['VTM10']
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
time_dim, lat_dim, lon_dim = vhm0.get_dims()
time_var = waveData.variables[time_dim.name]
times = num2date(time_var[:], time_var.units)
latitudes = waveData.variables[lat_dim.name][:]
longitudes = waveData.variables[lon_dim.name][:]

output_dir = '../'

# ==========================================================================
# Write data as a CSV table with 4 columns: time, latitude, longitude, value
# ==========================================================================
filename = os.path.join(output_dir, 'data/CMEMS_ERA5.csv')
print(f'Writing data in tabular form to {filename} (this may take some time)...')

times_grid, latitudes_grid, longitudes_grid = [
    x.flatten() for x in np.meshgrid(times, latitudes, longitudes, indexing='ij')]

print("times_grid", len(times_grid))
print("latitudes_grid", len(latitudes_grid))
print("longitudes_grid", len(longitudes_grid))

df = pd.DataFrame({
    'id': range(1, 1 + len(times_grid)),
    'time': [t.isoformat(sep=" ") for t in times_grid],
    'latitude': latitudes_grid,
    'longitude': longitudes_grid,
    'vhm0': vhm0[:].flatten(),
    'vmdr': vmdr[:].flatten(),
    'vtm10': vtm10[:].flatten(),
    'u10': u10[:].flatten(),
    'v10': v10[:].flatten(),
    'speed': wind_speed[:].flatten(),
    'direction': wind_dir[:].flatten()
})

# Convert DataFrame to a list of dictionaries
records = df.to_dict(orient='records')

# Convert DataFrame to JSON
df_json = df.to_json(orient='records')

# Insert the records into MongoDB collection
mycol.insert_many(records)

producer.send('topic_test', value=df_json)

# document = {
#     'id': int(1),
#     # 'time': parse_datetime('2023-05-12 12:00:00'),
#     'latitude': float(30.166666666666657),
#     'longitude': float(-7.083333333333343),
#     'vhm0': float(0.0),
#     'vmdr': float(180.0),
#     'vtm10': float(0.0)
# }

# for j in range(9999):
#     print("Iteration", j)
#     data = {'counter': j}
#     producer.send('topic_test', value=data)
#     # Insert the message into MongoDB collection
#     mycol.insert_one(document)
#
#     print("Inserted message into MongoDB:", document)
#     sleep(0.5)


import os
import glob

# Specify the directory you want to delete files from
folder_path = './data'

# Use glob to match all .nc and .csv files in the directory
files = glob.glob(f'{folder_path}/*.nc') + glob.glob(f'{folder_path}/*.csv')

# Use os.remove to delete each file
for f in files:
    os.remove(f)

print('Done')