# %load_ext sparkmagic.magics

# +
import os
from IPython import get_ipython
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

username = os.environ['RENKU_USERNAME']
server = "http://iccluster029.iccluster.epfl.ch:8998"

# set the application name as "<your_gaspar_id>-homework3"
get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-final-project", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G"}}""".format(username)
)

# -

get_ipython().run_line_magic(
    "spark", "add -s {0}-final-project -l python -u {1} -k".format(username, server)
)

# + language="spark"
# istdaten = spark.read.orc("/data/sbb/part_orc/istdaten")
# istdaten.printSchema()
# + language="spark"
# istdaten.show(5)


# + language="spark"
# transfers = spark.read.orc("/data/sbb/part_orc/transfers")
# transfers.printSchema()

# + language="spark"
# transfers.show(5)

# + language="spark"
# calendar = spark.read.orc("/data/sbb/part_orc/calendar")
# calendar.printSchema()

# + language="spark"
# calendar = spark.read.orc("/data/sbb/part_orc/calendar")
# calendar.printSchema()
# -

# # Stops processing

# + language="spark"
# stops = spark.read.csv("/data/sbb/csv/allstops/stop_locations.csv")

# + language="spark"
# from functools import reduce
#
# oldColumns = stops.schema.names
# newColumns = ["STOP_ID", "STOP_NAME", "STOP_LAT", "STOP_LON", "LOCATION_TYPE", "PARENT_STATION"]
#
# stops = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), stops)
# stops.printSchema()
# stops.show()

# + language="spark"
# from math import sin, cos, sqrt, atan2, radians
# import pyspark.sql.functions as F
#
# @F.udf
# def distance_gps(coordinate_struct):
#     """Return the distance between two GPS coordinates in km"""
#     
#     # approximate radius of earth in km
#     R = 6373.0
#     
#     lat1=radians(float(coordinate_struct[0]))
#     lon1=radians(float(coordinate_struct[1]))
#     lat2=radians(float(coordinate_struct[2]))
#     lon2=radians(float(coordinate_struct[3]))
#     
#     dlon = lon2 - lon1
#     dlat = lat2 - lat1
#
#     #StackOverflow : https://stackoverflow.com/a/19412565
#     a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#
#     return R * c

# + language="spark"
# hb_df = stops.filter(stops.STOP_NAME == "Zürich HB")
# hb_df.show(1)

# + language="spark"
# ZURICH_HB_LAT=47.3781762039461
# ZURICH_HB_LON=8.54021154209037
#
# stops_hb = stops.withColumn('ZHB_LAT', F.lit(ZURICH_HB_LAT))\
#     .withColumn('ZHB_LON', F.lit(ZURICH_HB_LON))
#     
# stops_hb = stops_hb.withColumn('distance_hb',distance_gps(F.struct(stops_hb.STOP_LAT, stops_hb.STOP_LON, stops_hb.ZHB_LAT, stops_hb.ZHB_LON)))

# + language="spark"
# stops_hb.sample(0.001).show(30)

# + language="spark"
# THRESHOLD = 17
# stops_in_radius = stops_hb.filter(stops_hb.distance_hb<THRESHOLD)
# stops_in_radius.count()
# -

# # Stop times

# + language="spark"
# stopt = spark.read.orc("/data/sbb/part_orc/stop_times")
# stopt.printSchema()

# + language="spark"
# stopt.show()

# + language="spark"
# stopt.count()

# + language="spark"
# stopt = stopt.filter("year == 2020").filter("month == 5").filter("day >= 13").filter("day < 17")
# stopt.count()

# + language="spark"
# stoptime_in_radius = stopt.join(stops_in_radius, on="stop_id",how="inner")
# print(stoptime_in_radius.count())
# stoptime_in_radius.show()

# + magic_args="-o stop_id_in_radius_list" language="spark"
# stop_id_in_radius_list = stops_in_radius.select(stops_in_radius.STOP_ID)
# stop_id_in_radius_list.write.save('/user/benhaim/final-project/stop_ids_in_radius.csv', format='csv', mode='append')
# -

stop_id_in_radius_list.to_csv("../data/stop_ids_in_radius.csv", index=False)

# # Walking distances

# + language="spark"
# print(stops_in_radius.count())
# stops_in_radius.show()

# + language="spark"
# stopw = stops_in_radius.select(stops_in_radius.STOP_ID, stops_in_radius.STOP_NAME, stops_in_radius.STOP_LAT, stops_in_radius.STOP_LON, stops_in_radius.PARENT_STATION)
# stopw.show()

# + language="spark"
#
# stopw2 = stopw.withColumnRenamed("STOP_ID","STOP_ID_2").withColumnRenamed("STOP_NAME","STOP_NAME_2").withColumnRenamed("STOP_LAT","STOP_LAT_2")\
#     .withColumnRenamed("STOP_LON","STOP_LON_2").withColumnRenamed("PARENT_STATION","PARENT_STATION_2")
# stopw_cross = stopw.crossJoin(stopw2)
# size = stopw_cross.count()
# print(size, 2394*2394, "Equal : ", size==2394*2394)
# stopw_cross.show()

# + language="spark"
# max_walk_distance_km = 0.5
# stopw_dist = stopw_cross.withColumn('walk_distance',distance_gps(F.struct(stopw_cross.STOP_LAT, stopw_cross.STOP_LON, stopw_cross.STOP_LAT_2, stopw_cross.STOP_LON_2)))
# stopw_dist_500m = stopw_dist.filter(stopw_dist.walk_distance<=max_walk_distance_km).filter(stopw_dist.STOP_NAME!=stopw_dist.STOP_NAME_2).cache()
# stopw_dist_500m.show()

# + language="spark"
# stopw_dist_500m.sample(0.001).show(50)

# + language="spark"
# stopw_dist_500m.count()

# + magic_args="-o stopw_dist_500m -n -1" language="spark"
# stopw_dist_500m
# -

stopw_500m = stopw_dist_500m
(stopw_500m["STOP_NAME"]!=stopw_500m["STOP_NAME_2"]).sum()

(stopw_500m["walk_distance"]!=0.0).sum()

for index, row in stopw_500m.iterrows():
    if (index<20):
        print(row['STOP_NAME'], row['STOP_NAME_2'], row['walk_distance'])

# +
from collections import defaultdict
import math

walk_dict = defaultdict(list)

filter_self_walk=True

stopw_500m = stopw_500m.drop_duplicates(subset=["STOP_NAME", "STOP_NAME_2"])

if filter_self_walk:
    stopw_500m=stopw_500m[stopw_500m["STOP_NAME"]!=stopw_500m["STOP_NAME_2"]]

# 50m/ minute walking speed, computing m.s-1
walk_speed = 50 / 60
    
for index, row in stopw_500m.iterrows():
    walk_dict[row['STOP_NAME']].append((row['STOP_NAME_2'], row['walk_distance']*walk_speed*1000))

#for key in walk_dict:
#    print("===STOP", key,"===")
#    for (ostop, dist) in walk_dict[key]:
#        print(ostop, "at a distance of ", math.ceil(dist*1000),"m")
# +
from typing import List, Tuple

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.arr_time: int = -1  # earliest arrival time
        self.previous_node: Node = None  # previous node on shortest path
        self.acc_success: float = 0  # current accumulated success

class Stop(Node):
    def __init__(self, node_id, stop_name, station):
        super().__init__(node_id)
        self.stop_name: str = stop_name
        self.station: Station = station

class WalkingStop(Stop):
    def __init__(self, node_id, stop_name, station, neighbors):
        super().__init__(node_id, stop_name, station)
        self.neighbors = neighbors
            
    def set_neighbors(self, neighbors):
        self.neighbors = neighbors


# +
walkingStopsList = []

for key in walk_dict:
    walkingStopsList.append(WalkingStop(key+"_node", key+"_walk", key, None))
    
for walkingStop in walkingStopsList:
    station_name = walkingStop.station
    neighbors = walk_dict[station_name]
    walkingStop.set_neighbors(neighbors)
# -

for walkingStop in walkingStopsList:
    print(walkingStop.station+" neighbors :")
    for neigh in walkingStop.neighbors:
        print(str(neigh[0])+" : "+str(neigh[1]/60)+"min")




