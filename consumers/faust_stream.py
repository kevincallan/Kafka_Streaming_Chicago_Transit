"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


#  Faust Stream that ingests data from the Kafka Connect stations topic and places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Input Topic
topic = app.topic("jdbc.stations", value_type=Station)


# Output Topics
out_topic = app.topic("faust.stations.transformed", partitions=1, value_type=TransformedStation)

table = app.Table(
    "stations.transformation.table",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


# Using Faust, transforming input `Station` records into `TransformedStation` records.
# Example : If the `Station` record has the field `red` set to true, then set the `line` of the `TransformedStation` record to the string `"red"`


        
def station_process(station):   
    if station.red:
        line = 'red'
    elif station.blue:
        line = 'blue'
    elif station.blue:
        line = 'green'
    else:
        line = 'null'
    transformed_station = TransformedStation(station_id=station.station_id, station_name=station.station_name, order=station.order, line=line)
    return transformed_station


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#

@app.agent(topic)
async def station(stations):
    async for st in stations:
        transformed_station = station_process(st)
        table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()    
    
    
    
