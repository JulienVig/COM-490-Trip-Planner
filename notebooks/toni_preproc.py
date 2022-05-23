# # Create Spark Session

import os
# %load_ext sparkmagic.magics
from datetime import datetime
username = os.environ['RENKU_USERNAME']
server = server = "http://iccluster029.iccluster.epfl.ch:8998"
from IPython import get_ipython
get_ipython().run_cell_magic('spark', line="config", 
                             cell="""{{ "name":"{0}-final_project", "executorMemory":"4G", "executorCores":4, "numExecutors":10 }}""".format(username))

# + language="spark"
# ## SPARK IMPORTS
# from functools import reduce
# from pyspark.sql.functions import col, lit, unix_timestamp, from_unixtime, collect_list
# from pyspark.sql.functions import countDistinct, concat
# from pyspark.sql.functions import udf, explode
# from pyspark.sql.types import ArrayType, StringType
# -

get_ipython().run_line_magic(
    "spark", "add -s {0}-final_project -l python -u {1} -k".format(username, server)
)

# ## Loading selected stations

# + language="spark"
#
# stations = spark.read.csv("/data/sbb/csv/allstops/stop_locations.csv")
# oldColumns = stations.schema.names
# newColumns = ["STOP_ID", "STOP_NAME", "STOP_LAT", "STOP_LON", "LOCATION_TYPE", "PARENT_STATION"]
#
# stations = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), stations)
# stations.printSchema()
# stations.show()
# -
# ## Building timetable


# + language="spark"
# stopt = spark.read.orc("/data/sbb/part_orc/stop_times")
# sel_stops = spark.read.csv("/user/benhaim/final-project/stop_ids_in_radius.csv")
# stopt.show(5)
# sel_stops.show(5)
# -
# Few numbers :


# + language="spark"
#
# ## number of trips
# print("Number of stop times : ")
# print(stopt.count())
# print("Number of trips :")
# stopt.select(countDistinct("trip_id")).show()
# print("Number of stops :")
# stopt.select(countDistinct("stop_id")).show()
# -

# ### Select the precise week : 

# + language="spark"
# stopt = stopt.filter("year == 2020").filter("month == 5").filter("day >= 13").filter("day < 17").cache()
# -

# ### We select the stops within range

# + language="spark"
#
# stopt = stopt.select(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "drop_off_type","year", "month", "day"])
# stopt = stopt.join(sel_stops, sel_stops._c0 == stopt.stop_id, "inner")
# -

# ### Selecting timestamps

# + language="spark"
#
# stopt = stopt.withColumn("arrival_time_complete", \
#                          concat(col("year"), lit("/"), col("month"), lit("/"), col("day"), lit(" "), col("arrival_time")))
# stopt = stopt.withColumn("departure_time_complete", \
#                          concat(col("year"), lit("/"), col("month"), lit("/"), col("day"), lit(" "), col("departure_time")))
#
# stopt = stopt.withColumn('arrival_time_complete_unix', unix_timestamp('arrival_time_complete', "yyyy/MM/dd HH:mm:ss"))
# stopt = stopt.withColumn('departure_time_complete_unix', unix_timestamp('departure_time_complete', "yyyy/MM/dd HH:mm:ss"))
# stopt = stopt.cache()
#
# stopt.select(["arrival_time_complete", "departure_time_complete"]).show()

# + language="spark"
# finalCols = ["trip_id", "arrival_time_complete", "departure_time_complete", "arrival_time_complete_unix", "departure_time_complete_unix", "stop_id"]
# stopt = stopt.select(finalCols)
# stopt.show()

# + language="spark"
# stopt.count()
# -

# ## Difference between arrival time and departure time

# + magic_args="-o distrib_time_stop" language="spark"
# stopt = stopt.withColumn("ms_diff", (col("departure_time_complete_unix") - col("arrival_time_complete_unix")))
# distrib_time_stop = stopt.groupBy("ms_diff").count()

# +
import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(15, 6))
distrib_time_stop["min_diff"] = distrib_time_stop["ms_diff"] / 60
g = sns.barplot(data=distrib_time_stop.sort_values("min_diff"), x="min_diff", y="count")
g.set_xticklabels(g.get_xticklabels(), rotation=45)
g.set_yscale("log")
# -

# ### Incorpore route id

# + language="spark"
# ## keep arrival
# trips = spark.read.orc("/data/sbb/part_orc/trips").select(["route_id", "trip_id"])
# trips.show(5)
# timetable = stopt.join(trips, "trip_id", "inner")
#
#
#

# + language="spark"
# ## adding stationName
# stations = stations.withColumn("station_id", stations.STOP_ID).drop("STOP_ID")
# timetableRouteArrival = timetable.join(stations, stations.station_id == timetable.stop_id)
# timetableRouteArrival = timetableRouteArrival.withColumn("route_stop_id", concat(col("route_id"), lit("$"), col("STOP_NAME"))).select(["route_stop_id", "arrival_time_complete_unix", "departure_time_complete_unix", "route_id", "stop_id", "trip_id"])
# timetableRouteArrival = timetableRouteArrival.dropDuplicates().cache()
# print(timetableRouteArrival.count())
# print(timetableRouteArrival.show(10))
# -

# ### We duplicate the routeStops into arrival and departure

# + magic_args="-o waiting_times" language="spark"
# duplicate_marking_function = udf(lambda n : ["A", "D"], ArrayType(StringType()))
# allTimetableRouteArrival = timetableRouteArrival.withColumn("duplicate_mark", duplicate_marking_function(timetableRouteArrival.route_id))
# allTimetableRouteArrival = allTimetableRouteArrival.withColumn("end", explode(allTimetableRouteArrival.duplicate_mark)).drop("duplicate_mark")
# allTimetableRouteArrival = allTimetableRouteArrival.withColumn("end_route_stop_id", concat(col("route_stop_id"), lit("$"), col("end"))).dropDuplicates().cache()
# waiting_times = allTimetableRouteArrival.withColumn("wait_weight", col("departure_time_complete_unix") - col("arrival_time_complete_unix")).select(["wait_weight", "route_stop_id"]).dropDuplicates().cache()
# allTimetableRouteArrival.show()

# + language="spark"
# waiting_times.show() 
# print("number of waiting edges : ")
# print(waiting_times.count())
# allTimetableRouteArrival.show()
# print("number of routeStops :")
# print(allTimetableRouteArrival.count())
# -

waiting_times.to_csv("../data/waiting_times.csv")

## too long
# %%spark -o allTimetableRouteArrival -n -1
allTimetableRouteArrival

# + language="spark"
# allTimetableRouteArrival.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save("/user/magron/sbb_prep_data/timetableFixed.csv")
# -

# ## Construction of station

# + language="spark"
# stations

# + language="spark"
#
# stations = stations.join(sel_stops, sel_stops._c0 == stations.station_id, "inner").cache()
# print(stations.count())
# timetables = allTimetableRouteArrival
# stations.show()

# + language="spark"
#
# stations = stations.select(["STOP_NAME", "STOP_LAT", "STOP_LON", "station_id"])
# routeStops = timetables.select(["end_route_stop_id", "stop_id"]).dropDuplicates()
#
# stationsDB = routeStops.join(stations, stations.station_id == routeStops.stop_id)\
#         .groupBy(["station_Id", "STOP_NAME", "STOP_LAT", "STOP_LON"])\
#         .agg(collect_list("end_route_stop_id")).cache()
# stationsDB.show()


# + language="spark"
# stationsDB.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save("/user/magron/sbb_prep_data/stations.csv")

# + magic_args="-o stationsDB" language="spark"
# stationsDB
# -

stationsDB.to_csv("../data/stations.csv")

# ## Building Stops 
#
# RouteStops : 
#
# - id, 
# - next_stop, 
# - travel_time,
# - idx_on_rout

# + language="spark"
# routeStops = routeStops.join(stations, routeStops.stop_id == stations.station_id, "left").dropDuplicates().cache()

# + language="spark"
# routeStops.show(5)
# -




