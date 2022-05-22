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

# +
lat1 = 52.2296756
lon1 = 21.0122287
lat2 = 52.406374
lon2 = 16.9251681

print("Result:", distance_gps(lat1, lon1, lat2, lon2))
print("Should be:", 278.546, "km")

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
# -

stop_id_in_radius_list.to_csv("../data/stop_ids_in_radius.csv", index=False)

# # Walking distances

# + language="spark"
# print(stops_in_radius.count())
# stops_in_radius.show()
# -




