# Scala realtime vehicles simulator from static GTFS

This Scala application, starting from static GTFS (General Transit Feed Specification) files, calculates all in transit vehicles positions in realtime.

It outputs all vehicles current latitude and longitude and timestamp.

The simulator doesn't take care of the GTFS "shapes.txt" optional file to calculate vehicle position, it simply interpolates between stops latitude/longitude.

## Running

Go to the main project folder and then:
```
sbt run
```