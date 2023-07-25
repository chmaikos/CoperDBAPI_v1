# CoperDBAPI

<p align="center">
  <img width="870" height="700" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/Screenshot%202023-07-24%20at%203.33.34%20PM.png" alt="CoperDBAPI"/>
</p>


### What is the CoperDBAPI?

A traclet is an image representation of a trajectory. This representation is indicative of the mobility patterns of the moving objects. TraClets need to efficiently visualize and capture two key features that characterize the trajectory patterns of moving objects: i) the shape of the trajectory which indicates the way the object moves in space, and ii) the speed that indicates how fast the object moves in space.

---

#### Example Usage


###### In the same folder with docker-compose.yml
```shell
docker-compose up --build
```

###### If you want to be sure for orphans containers you can do
```shell
docker-compose up --build --remove-orphans
```
<p align="center">
  <img width="846" height="500" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/Screenshot%202023-07-24%20at%205.32.18%20PM.png" alt="Sublime's custom image"/>
</p>

### ProducerWave

The first time it will pull data from the Copernicus is when it is first uploaded the docker composes.
After from that it will take data every 3 hours.
Duplicates don't exist because the time that pull data from Copernicus is :
```code
current_time - 3hours + 1second until current time
```

Copernicus has information every 3hours started at 00.00
```example
If the program starts at 05.00 o'clock that means that the first time that gets is 5 - (5%2) = 3 --> 03.00 o'clock
```

We get the information as a .nc file from Copernicus, we refactor it into json and push it into kafka topic
```example
wave_topic
```
and into MongoDB into a collection named

```example
waveData
```
with the below format
```
{
  "time": "2023-07-24 12:00:00",
  "latitude": 35,
  "longitude": 18.916666666666657,
  "vhm0": 0.25999999046325684,
  "vmdr": 322.69000244140625,
  "vtm10": 3.4600000381469727
}
```
The information analyzes below:
```
Significant Wave Height (VHM0)
Wave Direction (VMDR)
Wave period mean value (VTM10)
```

<p align="center">
  <img width="870" height="700" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/Screenshot%202023-07-24%20at%203.33.34%20PM.png" alt="CoperDBAPI"/>
</p>
---

### ProducerWind

The first time it will pull data from the Copernicus is when it is first uploaded the docker composes.
After from that it will take data every 1 day.
The erliest data that we can get from Copernicus is 6 days ago.
The values of time in which we have access are the follows:
```
'time': [
                    '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00', '21:00', '22:00', '23:00',
                ]
```
Duplicates don't exist.

We get the information as a .nc file from Copernicus, we refactor it into json and push it into kafka topic
```example
wind_topic
```
and into MongoDB into a collection named

```example
windData
```
with the below format
```
{
  "time": "2023-07-18 00:00:00",
  "latitude": 50.150001525878906,
  "longitude": -27.1200008392334,
  "u10": -4.6063704822533245,
  "v10": -0.529921079222938,
  "speed": 4.636751596751709,
  "direction": 83.43748990096958
}
```

The information analyzes below:
```
East wind component (u10)
North wind component (v10)
Speed is a combination of the above two components (speed)
```

Speed information :

<p align="center">
  <img width="500" height="300" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/Screenshot%202023-07-25%20at%208.25.44%20AM.png" alt="CoperDBAPI"/>
</p>

---

### API

## Request

`GET /data`

    http://127.0.0.1:5000/data?dateMin=2023-07-24T04:00:00&dateMax=2023-07-24T07:00:00&latitude=50&longitude=-27&radius=10

    User has to fill 5 variables : dateMin, dateMax, latitude, longitude, radius

## Response

    { "waveData" :
        {
          "time": "2023-07-24 12:00:00",
          "latitude": 35,
          "longitude": 18.916666666666657,
          "vhm0": 0.25999999046325684,
          "vmdr": 322.69000244140625,
          "vtm10": 3.4600000381469727
        }
    }

    { "windData" :
        {
           "time": "2023-07-18 00:00:00",
           "latitude": 50.150001525878906,
           "longitude": -27.1200008392334,
           "u10": -4.6063704822533245,
           "v10": -0.529921079222938,
           "speed": 4.636751596751709,
           "direction": 83.43748990096958
        }
    }
