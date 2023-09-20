import os
import logging
from netCDF4 import Dataset, num2date
import numpy as np
import pandas as pd
import cdsapi
import configparser
from datetime import datetime, timedelta
import time
from math import pi, radians, cos
from confluent_kafka import Producer
import json
import pymongo

# Configure logging
logging.basicConfig(level=logging.INFO, filename='app.log',
                    filemode='w', format='%(name)s-%(levelname)s-%(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s-%(levelname)s-%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def delivery_report(err, msg):
    if err is not None:
        logging.error('Failed to deliver message: %s', err)
    else:
        logging.info('Message delivered to topic: %s', msg.topic())


def create_square(latitude, longitude, radius):
    earth_radius = 6371.0
    lat_rad = radians(latitude)
    radius_deg = radius / earth_radius
    min_latitude = latitude - radius_deg
    max_latitude = latitude + radius_deg
    min_longitude = longitude - (radius_deg / cos(lat_rad))
    max_longitude = longitude + (radius_deg / cos(lat_rad))
    return min_latitude, min_longitude, max_latitude, max_longitude


producer = Producer({'bootstrap.servers': 'kafka1:29092'})
topic_metadata = producer.list_topics()
topic_list = topic_metadata.topics
for topic in topic_list:
    logging.info("----------------------------------------------- %s", topic)
topic = 'wind_topic'
myclient = pymongo.MongoClient("mongodb://mongodb:27017")
db = myclient["kafka_db"]
mycol = db["windData"]

while True:
    try:
        config = configparser.ConfigParser()
        config.read('config.conf')
        lon, lat, rad = map(float,
                            [config.get('Default', 'longitude'),
                             config.get('Default', 'latitude'),
                             config.get('Default', 'radius')]
                            )
        lat_min, lon_min, lat_max, lon_max = create_square(lat, lon, rad)
        curr_time = datetime.now() - timedelta(days=6)
        mon_curr, day_curr = curr_time.strftime("%m"), curr_time.strftime("%d")
        c = cdsapi.Client()
        windData = 'data/ERA5_Wind3H.nc'
        dataList = [{'fileLocation': windData,
                     'month': mon_curr,
                     'day': day_curr}]
        logging.info("month %s", dataList[0]["month"])
        logging.info("day %s", dataList[0]["day"])

        for item in dataList:
            c.retrieve('reanalysis-era5-single-levels',
                       {'product_type': 'reanalysis',
                        'variable': ['10m_u_component_of_wind',
                                     '10m_v_component_of_wind'],
                        'year': '2023',
                        'month': item['month'],
                        'day': item['day'],
                        'time': [f'{i:02d}:00' for i in range(24)],
                        'area': [lat_max,
                                 lon_min,
                                 lat_min,
                                 lon_max],
                        'format': 'netcdf'},
                       item['fileLocation'])

        with Dataset('data/ERA5_Wind3H.nc', 'r+') as windData_BL:
            u10, v10 = map(windData_BL.variables.get, ['u10', 'v10'])
            wind_speed = np.sqrt(u10[:]**2 + v10[:]**2)
            wind_dir = (270 - np.arctan2(v10[:], u10[:]) * 180 / pi) % 360
            time_dim, lat_dim, lon_dim = u10.get_dims()
            time_var = windData_BL.variables[time_dim.name]
            times = num2date(time_var[:], time_var.units)
            latitudes = windData_BL.variables[lat_dim.name][:]
            longitudes = windData_BL.variables[lon_dim.name][:]
            times_grid, latitudes_grid, longitudes_grid = [x.flatten()
                                                           for x
                                                           in np.meshgrid(
                                                               times,
                                                               latitudes,
                                                               longitudes,
                                                               indexing='ij')]

            df = pd.DataFrame({'time': [t.isoformat(sep=" ")
                                        for t in times_grid],
                               'latitude': latitudes_grid,
                               'longitude': longitudes_grid,
                               'u10': u10[:].flatten(),
                               'v10': v10[:].flatten(),
                               'speed': wind_speed.flatten(),
                               'direction': wind_dir.flatten()})

            logging.info(df)
            df.dropna(subset=['u10'], inplace=True)
            logging.info(df)
            data = df.to_dict(orient='records')
            mycol.insert_many(data)

        for index, row in df.iterrows():
            value = json.dumps(row.to_dict()).encode('utf-8')
            producer.produce(topic=topic,
                             value=value,
                             callback=delivery_report)
            producer.flush()

        os.remove(windData)
        logging.info("File deleted successfully.")
    except FileNotFoundError:
        logging.error("File not found.")
    except Exception as e:
        logging.error("Error: %s", e)
    time.sleep(24 * 60 * 60)
