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
  "_id": {
    "$oid": "64be7b2a8aeed4c895d26bbc"
  },
  "time": "2023-07-24 12:00:00",
  "latitude": 35,
  "longitude": 18.916666666666657,
  "vhm0": 0.25999999046325684,
  "vmdr": 322.69000244140625,
  "vtm10": 3.4600000381469727
}
```
---
