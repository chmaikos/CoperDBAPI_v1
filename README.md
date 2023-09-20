# CoperDBAPI

![CoperDBAPI](https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-24%20at%203.33.34%20PM.png)

- [CoperDBAPI](#coperdbapi)
  - [What is the CoperDBAPI?](#what-is-the-coperdbapi)
    - [Example Usage](#example-usage)
    - [ProducerWave](#producerwave)
    - [ProducerWind](#producerwind)
    - [API](#api)
      - [Request](#request)
      - [Response](#response)

## What is the CoperDBAPI?

Lorem impsum dolor sit amet, consectet

---

### Example Usage

In the same folder with docker-compose.yml

```sh
docker-compose up --build
```

To ensure there are no orphan containers, you can use

```sh
docker-compose up --build --remove-orphans
```

![Sublime's custom image](https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-24%20at%205.32.18%20PM.png)

### ProducerWave

The first time it will pull data from Copernicus is when the docker compose is first uploaded. After that, it will retrieve data every 3 hours. Duplicates do not exist because the time range for pulling data from Copernicus is: `current_time - 3 hours + 1 second`

Copernicus updates information every 3 hours starting at 00:00. If the program starts at 05:00 o'clock, that means that the first time it retrieves data is at: `5 - (5%3) = 2` --> 02:00 o'clock.

We obtain the information as a .nc file from Copernicus, refactor it into JSON, and push it into the Kafka topic `wave_topic` and into MongoDB in a collection named `waveData` with the following format:

```json
{
  "time": "2023-07-24 12:00:00",
  "latitude": 35,
  "longitude": 18.916666666666657,
  "vhm0": 0.25999999046325684,
  "vmdr": 322.69000244140625,
  "vtm10": 3.4600000381469727
}
```

The information is analyzed below:

| Variable | Description                  | Unit |
|----------|------------------------------|------|
| vhm0     | Significant Wave Height      |meters|
| vmdr     | Wave Direction               |      |
| vtm10    | Wave Period Mean Value       |      |

![CoperDBAPI](https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%208.24.36%20AM.png)

The horizontal resolution is: `0.083° x 0.083°`

---

### ProducerWind

The first time it will pull data from Copernicus is when the docker compose is first uploaded. After that, it will retrieve data every day. The earliest data that we can get from Copernicus is from 6 days ago. The available time values are as follows:

```json
'time': [
  '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
  '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
  '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
  '18:00', '19:00', '20:00', '21:00', '22:00', '23:00',
]
```

Duplicates do not exist.

We obtain the information as a .nc file from Copernicus, refactor it into JSON, and push it into the Kafka topic `wind_topic` and into MongoDB in a collection named `wind_data` with the following format:

```json
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

The information is analyzed below:

| Variable | Description                             | Unit  |
|----------|-----------------------------------------|-------|
| u10      | East Wind Component                     | m/s   |
| v10      | North Wind Component                    | m/s   |
| speed    | Combination of the above two components | m/s   |

Speed information:

![CoperDBAPI](https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%208.25.32%20AM.png)

![CoperDBAPI](https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%208.25.44%20AM.png)

The horizontal resolution is: `0.25° x 0.25°`

---

### API

#### Request

`GET /data?dateMin=2023-07-19T04:00:00&dateMax=2023-07-19T07:00:00&latitude=35&longitude=18&radius=20`

Users must provide 5 variables: `dateMin`, `dateMax`, `latitude`, `longitude`, `radius`.

![CoperDBAPI](https://github.com/ArtemisStefanidou/CoperDBAPI/blob/main/photos/Screenshot%202023-07-25%20at%2011.53.06%20AM.png)

#### Response

If the user provides a date older or newer than those in the collections, an empty list is returned.

When a valid date is provided, we check if data exists for the specified latitude and longitude. If data exists, information from both collections is returned.

```json
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
      },
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
      },
      {...}
    ]
  }
]
```

If not, an empty list is returned.
