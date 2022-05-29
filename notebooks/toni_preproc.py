# # Create Spark Session

import os
# %load_ext sparkmagic.magics
from datetime import datetime
username = os.environ['RENKU_USERNAME']
server = server = "http://iccluster029.iccluster.epfl.ch:8998"
from IPython import get_ipython
get_ipython().run_cell_magic('spark', line="config", 
                             cell="""{{ "name":"{0}-final_project", "executorMemory":"4G", "executorCores":4, "numExecutors":10 }}""".format(username))

get_ipython().run_line_magic(
    "spark", "add -s {0}-final_project -l python -u {1} -k".format(username, server)
)

# + language="spark"
# ## SPARK IMPORTS
# from functools import reduce
# from pyspark.sql.functions import col, lit, unix_timestamp, from_unixtime, collect_list
# from pyspark.sql.functions import countDistinct, concat
# from pyspark.sql.functions import udf, explode, split
# import pyspark.sql.functions as F
# from pyspark.sql.types import ArrayType, StringType, IntegerType
#
# REMOTE_PATH = "/group/abiskop1/project_data/"

# + language="spark"
#
#
#
# def count_nan_null(df):
#     df.select([F.count(F.when(F.isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()
#
#
# def read_orc(fname):
#     df = spark.read.orc("/data/sbb/part_orc/{name}".format(name=fname))
#     return df.filter((df.year == 2020) & (df.month == 5) & (df.day > 12) & (df.day < 18))

# + language="spark"
# spark.conf.set("spark.sql.session.timeZone", "UTC+2")
# -

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
# stopt = read_orc("stop_times").dropna()
# sel_stops = spark.read.csv("/user/benhaim/final-project/stop_ids_in_radius.csv")
# stopt.show(5)
# sel_stops.show(5)
# -
# Few numbers :


# + language="spark"
# count_nan_null(stopt)

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
# stopt = stopt.dropna()
# finalCols = ["trip_id", "arrival_time_complete", "departure_time_complete", "arrival_time_complete_unix", "departure_time_complete_unix", "stop_id", "stop_sequence"]
# stopt = stopt.select(finalCols)
# stopt.show()

# + language="spark"
# count_nan_null(stopt)
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
# trips = read_orc("trips").select(["route_id", "trip_id"])
# trips.show(5)
#
# timetable = stopt.join(trips, "trip_id", "inner")
# stopt = timetable
#
#
#

# + language="spark"
# # adding stationName
# stations = stations.withColumn("station_id", stations.STOP_ID).drop("STOP_ID")
# timetableRouteArrival = timetable.join(stations, stations.station_id == timetable.stop_id)
# timetableRouteArrival = timetableRouteArrival.withColumn("route_stop_id", concat(col("route_id"), lit("$"), col("STOP_NAME"), lit("$"), col("stop_sequence")))
# stopt = timetableRouteArrival
# timetableRouteArrival = timetableRouteArrival.select(["route_stop_id", "arrival_time_complete_unix", "departure_time_complete_unix", "route_id", "stop_id", "stop_sequence"])
#
# timetableRouteArrival = timetableRouteArrival.dropDuplicates()
# print(timetableRouteArrival.count())
# timetableRouteArrival.show(10)
# -

# ### We duplicate the routeStops into arrival and departure

# + magic_args="-o waiting_times" language="spark"
# duplicate_marking_function = udf(lambda n : ["A", "D"], ArrayType(StringType()))
# allTimetableRouteArrival = timetableRouteArrival.withColumn("duplicate_mark", duplicate_marking_function(timetableRouteArrival.route_id))
# allTimetableRouteArrival = allTimetableRouteArrival.withColumn("end", explode(allTimetableRouteArrival.duplicate_mark)).drop("duplicate_mark")
# allTimetableRouteArrival = allTimetableRouteArrival.withColumn("end_route_stop_id", concat(col("route_stop_id"), lit("$"), col("end"))).dropDuplicates().cache()
# routeStops = allTimetableRouteArrival
# waiting_times = allTimetableRouteArrival.withColumn("wait_weight", col("departure_time_complete_unix") - col("arrival_time_complete_unix")).select(["wait_weight", "route_stop_id"]).dropDuplicates().cache()
# allTimetableRouteArrival = allTimetableRouteArrival.drop("sequence_number")
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
# allTimetableRouteArrival = allTimetableRouteArrival.filter(allTimetableRouteArrival.end == "A")

# + language="spark"
# allTimetableRouteArrival.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "timetableFinal.csv")
# -

# ## Construction of station

# + language="spark"
#
# stations = stations.join(sel_stops, sel_stops._c0 == stations.station_id, "inner").cache()
# print(stations.count())
# timetables = allTimetableRouteArrival
# stations.show()

# + language="spark"
#
# stations = stations.select(["STOP_NAME", "STOP_LAT", "STOP_LON", "station_id"])
# routeStopsSimple = timetables.select(["end_route_stop_id", "stop_id"]).dropDuplicates()
#
# stationsDB = routeStopsSimple.join(stations, stations.station_id == routeStops.stop_id)\
#         .groupBy(["station_Id", "STOP_NAME", "STOP_LAT", "STOP_LON"])\
#         .agg(collect_list("end_route_stop_id")).cache()
# stationsDB.show()


# + magic_args="-o stationsDB -n -1" language="spark"
# stationsDB
# -

stationsDB.to_csv("../data/stations_ext.csv")

# ## Builing Stops

# + language="spark"
#
# stopTrips = stopt.select(["trip_id", "arrival_time_complete_unix", "departure_time_complete_unix", "stop_id", 'stop_sequence', "route_id", "STOP_NAME", "route_stop_id"]).dropDuplicates()
# duplicate_marking_function = udf(lambda n : ["A", "D"], ArrayType(StringType()))
# stopTrips = stopTrips.withColumn("duplicate_mark", duplicate_marking_function(stopTrips.route_id))
# stopTrips = stopTrips.withColumn("end", explode(stopTrips.duplicate_mark)).drop("duplicate_mark")
# stopTrips = stopTrips.withColumn("end_route_stop_id", concat(col("route_stop_id"), lit("$"), col("end"))).dropDuplicates().cache()
# stopTrips.cache()

# + language="spark"
# arrivals = stopTrips.filter(stopTrips.end == "A").drop("departure_time_complete_unix").withColumnRenamed("arrival_time_complete_unix", "reached_time")
# departures = stopTrips.filter(stopTrips.end == "D").drop("arrival_time_complete_unix").withColumnRenamed("departure_time_complete_unix", "reached_time")

# + language="spark"
# ## points to the matching end
# arrivals = arrivals.withColumn("matching_arrival", col("stop_sequence") - 1)
# targets = departures.select(["end_route_stop_id", "stop_sequence", "route_stop_id", "reached_time", "trip_id"])
# targets = targets.withColumnRenamed("end_route_stop_id", "target_end_route_stop_id")
# targets = targets.withColumnRenamed("stop_sequence", "target_sequence")
# targets = targets.withColumnRenamed("route_stop_id", "target_route_stop_id")
# targets = targets.withColumnRenamed("reached_time", "reached_time_target")
# targets = targets.withColumnRenamed("trip_id", "target_trip_id")
#
#
# terminus = arrivals.filter("matching_arrival == 0")\
#             .select(["STOP_NAME", "stop_sequence", "end_route_stop_id", "route_stop_id"])\
#             .dropDuplicates()\
#             .cache()
#
# arrivals = arrivals.filter("matching_arrival != 0")
# print("Number of arrivals before : ", arrivals.count())
# arrivals = arrivals.join(targets,  (targets.target_sequence == arrivals.matching_arrival)\
#                                      & (targets.target_trip_id == arrivals.trip_id), "left")
#
# print("Number of arrivals after : ", arrivals.count())
# arrivals = arrivals\
#             .select(["STOP_NAME", "reached_time", "stop_sequence", "end_route_stop_id", "route_stop_id", "reached_time_target", "target_end_route_stop_id"])\
#             .dropDuplicates()\
#             .withColumn("travel_time", col("reached_time") - col("reached_time_target"))\
#             .select(["STOP_NAME", "stop_sequence", "end_route_stop_id", "route_stop_id", "travel_time", "target_end_route_stop_id"])\
#             .dropDuplicates()
#
# cols = list(set(arrivals.schema.names) - {'travel_time'})
# arrivals = arrivals.groupBy(cols).min()\
#             .withColumnRenamed("min(travel_time)", "travel_time")\
#             .dropDuplicates()
#
# cols = list(set(arrivals.schema.names) - {'target_end_route_stop_id'})
# arrivals = arrivals.groupBy(cols).agg(F.first("target_end_route_stop_id"))\
#             .withColumnRenamed("first(target_end_route_stop_id, false)", "target_end_route_stop_id")\
#             .dropDuplicates()\
#             .cache()
#
#
# arrivals.show(5)
# -

# Excuse me but wtf

# + language="spark"
# #print("All arrival numbers : ", allArrivalNumbers)
# print("Number of starts : ", terminus.count())
# print("Number of arrival : ", arrivals.count())
# print(arrivals.count() + terminus.count())

# + magic_args="-o stop_seq_distrib" language="spark"
# stop_seq_distrib = arrivals.groupBy("stop_sequence").count().sort("stop_sequence")
# -

plt.figure(figsize=(15, 6))
g = sns.barplot(data=stop_seq_distrib, x="stop_sequence", y="count")

# + magic_args="-o travel_time_distrib" language="spark"
# travel_time_distrib = arrivals.groupBy("travel_time").count()
# -

plt.figure(figsize=(16, 5))
g = sns.barplot(data=travel_time_distrib, x="travel_time", y="count")
g.set_xticklabels(g.get_xticklabels(), rotation=45);

# ### Investigation on the 0 travel_time

# + magic_args="-o sample" language="spark"
#
# sample = arrivals.filter("travel_time == 0")
# -

sample[["end_route_stop_id", "target_end_route_stop_id"]].iloc[0]

# + language="spark"
# arrivals.groupBy("travel_time").count().show()
# -

# ### Adding 30 seconds to all the travel_time at 0

# + language="spark"
#
# adding_30 = udf(lambda time : 30 if(time == 0) else time)
# arrivals = arrivals.withColumn("travel_time", adding_30(arrivals.travel_time))
# arrivals.show(5)

# + magic_args="-o tt_distrib" language="spark"
# tt_distrib = arrivals.groupBy("travel_time").count().sort("travel_time")
# -

plt.figure(figsize=(20, 6))
sns.barplot(data=tt_distrib, x="travel_time", y="count")

# + language="spark"
# arrivals.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "arrivalsRouteStopsFinal.csv")

# + language="spark"
# terminus.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "terminusFinal.csv")
# -

# ### Dealing with departures

# + language="spark"
#
#
# departures = departures.join(waiting_times, "route_stop_id")\
#           .withColumn("target_end_route_stop_id", concat(col("route_stop_id"), lit("$A")))\
#           .withColumnRenamed("wait_weight", "travel_time")\
#           .select(["route_stop_id", "stop_id", "route_id", "STOP_NAME", "end_route_stop_id", "travel_time", "target_end_route_stop_id"])\
#           .dropDuplicates()
#
#
# cols = list(set(departures.schema.names) - {'travel_time'})
# departures = departures.groupBy(cols).min()\
#             .withColumnRenamed("min(travel_time)", "travel_time")\
#             .dropDuplicates()\
#             .cache()
#
# departures.show(5)       

# + language="spark"
#
# departures = departures.drop("stop_id").dropDuplicates()

# + language="spark"
# print("Number of starts : ", terminus.count())
# print("Number of arrival : ", arrivals.count())
# print(arrivals.dropDuplicates().count() + terminus.count())
# print(departures.count())

# + language="spark"
# departures.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "departuresFinal.csv")
# -
# ## Integrity test
#
#
# I don't understand but I guess I'll go with the flow

# + language="spark"
# routes = read_orc("routes")


# + language="spark"
# example = routes.filter("year == 2020").filter("month == 5").filter("day >= 13").filter("day < 17")
# print(example.drop("year", "month", "day").dropDuplicates().count())
# print(example.select("route_id").dropDuplicates().count())

# + language="spark"
# arrivals = spark.read.csv("/group/abiskop1/project_data/arrivalsRouteStops.csv", header=True)

# + language="spark"
# arrivals = arrivals.withColumn("route_id", F.split(arrivals.end_route_stop_id, "$").getItem(0))

# + language="spark"
# example = example.select("route_id").dropDuplicates().rdd
# route_ids = arrivals.select("route_id").dropDuplicates().rdd

# + language="spark"
#
# route_ids.map(lambda x : x.route_id).take(10)

# + language="spark"
# splitter = udf(lambda rid : rid.split("$")[0])
# route_stops = arrivals.withColumn("route_id", splitter(col("route_stop_id"))).select("route_id").dropDuplicates().rdd
# route_stops = route_stops.map(lambda x : x.route_id)
# example = example.map(lambda x : x.route_id)
# print(route_stops.count())
# route_stops.intersection(example).count()

# + language="spark"
# route_stops.take(10)
# + language="spark"
# print(timetableRouteArrival.show())
# timetableRouteArrival.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in timetableRouteArrival.columns]).show()
# -




# + language="spark"
#
# count_nan_null(arrivals)
# -


