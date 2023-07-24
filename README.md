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
  <img width="846" height="391" src="https://github.com/kontopoulos/TraClets/blob/main/traclet.png" alt="Sublime's custom image"/>
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
---
