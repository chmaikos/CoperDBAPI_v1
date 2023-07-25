# CoperDBAPI

<p align="center">
  <img width="870" height="700" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-24%20at%203.33.34%20PM.png" alt="CoperDBAPI"/>
</p>


### What is the CoperDBAPI?



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
  <img width="700" height="600" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-24%20at%205.32.18%20PM.png" alt="Sublime's custom image"/>
</p>

###### Keep docker compose running


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
  <img width="400" height="200" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%208.24.36%20AM.png" alt="CoperDBAPI"/>
</p>

The Horizontal Resolution is:
```
0.083° x 0.083°
```
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
  <img width="300" height="250" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%208.25.32%20AM.png" alt="CoperDBAPI"/>
</p>

<p align="center">
  <img width="150" height="50" src="https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%208.25.44%20AM.png" alt="CoperDBAPI"/>
</p>

The Horizontal Resolution is:
```
0.25° x 0.25°
```
---

### API

## Request

`GET /data`

    http://127.0.0.1:5000/data?dateMin=2023-07-19T04:00:00&dateMax=2023-07-19T07:00:00&latitude=35&longitude=18&radius=20

    User has to fill 5 variables : dateMin, dateMax, latitude, longitude, radius

## Response

When the user gives a date older or newer than those in the collections, returned an empty list.

    [
      {
        "waveData": [
          
        ]
      },
      {
        "windData": [
          
        ]
      }
    ]
    
When the user gives a valid date we check if it has data for this langitude, longitude given by the user.
If it has, it returns the information for both collections.

    [
      {
        "waveData": [
          {
            "time": "2023-07-19 06:00:00",
            "latitude": 35,
            "longitude": 18.916666666666657,
            "vhm0": 0.25999999046325684,
            "vmdr": 322.69000244140625,
            "vtm10": 3.4600000381469727
          }
          {...}
        ]
      },
      {
        "windData": [
          {
            "time": "2023-07-19 06:00:00",
            "latitude": 35,
            "longitude": 18.916666666666657,
            "vhm0": 0.25999999046325684,
            "vmdr": 322.69000244140625,
            "vtm10": 3.4600000381469727
          }
          {...}
        ]
      }
    ]

If not, it returns an empty list.

    [
      {
        "waveData": [
          
        ]
      },
      {
        "windData": [
          
        ]
      }
    ]
