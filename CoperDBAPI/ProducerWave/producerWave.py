import os
import logging
from netCDF4 import Dataset, num2date
import numpy as np
from motuclient import motu_api
import pandas as pd
import configparser
from datetime import datetime, timedelta
from pymongo import MongoClient
from confluent_kafka import Producer
import json
from math import radians, cos
import time

# Configure logging
logging.basicConfig(level=logging.INFO, filename='app.log',
                    filemode='w', format='%(name)s-%(levelname)s-%(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s-%(levelname)s-%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def create_square(latitude, longitude, radius):
    earth_radius = 6371.0
    lat_rad = radians(latitude)
    radius_deg = radius / earth_radius
    min_latitude = latitude - radius_deg
    max_latitude = latitude + radius_deg
    min_longitude = longitude - (radius_deg / cos(lat_rad))
    max_longitude = longitude + (radius_deg / cos(lat_rad))
    return min_latitude, min_longitude, max_latitude, max_longitude


class MotuOptions:
    def __init__(self, attrs: dict):
        self.attrs = attrs

    def __setattr__(self, k, v):
        self.attrs[k] = v

    def __getattr__(self, k):
        return self.attrs.get(k, None)


def motu_option_parser(script_template, usr, pwd, output_filename):
    dictionary = dict([e.strip().partition(" ")[::2]
                       for e in script_template.split('--')
                       ])
    dictionary['variable'] = [value
                              for (var, value)
                              in [e.strip().partition(" ")[::2]
                                  for e in script_template.split('--')
                                  ]
                              if var == 'variable'
                              ]

    for k, v in list(dictionary.items()):
        if v in ['<OUTPUT_DIRECTORY>',
                 '<OUTPUT_FILENAME>',
                 '<USERNAME>',
                 '<PASSWORD>']:
            dictionary[k] = {'<OUTPUT_DIRECTORY>': '.',
                             '<OUTPUT_FILENAME>': output_filename,
                             '<USERNAME>': usr, '<PASSWORD>': pwd}[v]
        if k in ['longitude-min',
                 'longitude-max',
                 'latitude-min',
                 'latitude-max',
                 'depth-min',
                 'depth-max']:
            dictionary[k] = float(v)
        if k in ['date-min', 'date-max']:
            dictionary[k] = v[1:-1]
        dictionary[k.replace('-', '_')] = dictionary.pop(k)
    dictionary.pop('python')
    dictionary['auth_mode'] = 'cas'
    return dictionary


def delivery_report(err, msg):
    if err is not None:
        logging.error('Failed to deliver message: %s', err)
    else:
        logging.info('Message delivered to topic: %s', msg.topic())


producer = Producer({'bootstrap.servers': 'kafka1:29092'})
topic_metadata = producer.list_topics()
topic_list = topic_metadata.topics
for topic in topic_list:
    logging.info("----------------------------------------------- %s", topic)
topic = 'wave_topic'
myclient = MongoClient("mongodb://mongodb:27017")
db = myclient["kafka_db"]
mycol = db["waveData"]

while True:
    try:
        config = configparser.ConfigParser()
        config.read('config.conf')
        lon, lat, rad = map(float,
                            [config.get('Default', 'longitude'),
                             config.get('Default', 'latitude'),
                             config.get('Default', 'radius')
                             ]
                            )
        lat_min, lon_min, lat_max, lon_max = create_square(lat, lon, rad)
        curr_time = datetime.now()
        delta_3h = curr_time - timedelta(hours=3) + timedelta(seconds=1)
        USERNAME, PASSWORD = 'mmini1', 'Artemis2000'
        OUTPUT_DIRECTORY = 'data'
        OUTPUT_FILENAME = 'CMEMS_Wave3H.nc'
        script_template = (
            f'python -m motuclient '
            f'--motu https://nrt.cmems-du.eu/motu-web/Motu '
            f'--service-id GLOBAL_ANALYSISFORECAST_WAV_001_027-TDS '
            f'--product-id cmems_mod_glo_wav_anfc_0.083deg_PT3H-i '
            f'--longitude-min {lon_min} '
            f'--longitude-max {lon_max} '
            f'--latitude-min {lat_min} '
            f'--latitude-max {lat_max} '
            f'--date-min "{delta_3h}" '
            f'--date-max "{curr_time}" '
            f'--variable VHM0 '
            f'--variable VMDR '
            f'--variable VTM10 '
            f'--out-dir {OUTPUT_DIRECTORY} '
            f'--out-name {OUTPUT_FILENAME} '
            f'--user {USERNAME} '
            f'--pwd {PASSWORD}'
        )

        logging.info(script_template)
        data_request_options_dict_automated = motu_option_parser(
            script_template, USERNAME, PASSWORD, OUTPUT_FILENAME)

        motu_api.execute_request(MotuOptions(
            data_request_options_dict_automated
            ))

        with Dataset('data/CMEMS_Wave3H.nc', 'r+') as waveData:
            waveData.set_auto_mask(True)
            vhm0, vmdr, vtm10 = map(waveData.variables.get,
                                    ['VHM0', 'VMDR', 'VTM10'])
            time_dim, lat_dim, lon_dim = vhm0.get_dims()
            time_var = waveData.variables[time_dim.name]
            times = num2date(time_var[:], time_var.units)
            latitudes = waveData.variables[lat_dim.name][:]
            longitudes = waveData.variables[lon_dim.name][:]
            times_grid, latitudes_grid, longitudes_grid = [x.flatten()
                                                           for x
                                                           in np.meshgrid(
                                                               times,
                                                               latitudes,
                                                               longitudes,
                                                               indexing='ij'
                                                               )
                                                           ]
            df = pd.DataFrame({'time': [t.isoformat(sep=" ")
                                        for t in times_grid],
                               'latitude': latitudes_grid,
                               'longitude': longitudes_grid,
                               'vhm0': vhm0[:].flatten(),
                               'vmdr': vmdr[:].flatten(),
                               'vtm10': vtm10[:].flatten()
                               })
            logging.info(df)
            df.dropna(subset=['vhm0'], inplace=True)
            logging.info(df)
            data = df.to_dict(orient='records')
            mycol.insert_many(data)
        for index, row in df.iterrows():
            value = json.dumps(row.to_dict()).encode('utf-8')
            producer.produce(topic=topic,
                             value=value,
                             callback=delivery_report)
            producer.flush()
        os.remove(OUTPUT_FILENAME)
        logging.info("File deleted successfully.")
    except FileNotFoundError:
        logging.error("File not found.")
    except Exception as e:
        logging.error("Error: %s", e)
    time.sleep(3 * 3600)
