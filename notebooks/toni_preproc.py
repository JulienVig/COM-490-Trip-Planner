# # Create Spark Session

# +
import os
# %load_ext sparkmagic.magics
from datetime import datetime
username = os.environ['RENKU_USERNAME']
server = server = "http://iccluster029.iccluster.epfl.ch:8998"
from IPython import get_ipython
get_ipython().run_cell_magic('spark', line="config", 
                             cell="""{{ "name":"{0}-final_project", "executorMemory":"4G", "executorCores":4, "numExecutors":10 }}""".format(username))

REMOTE_PATH = "/group/abiskop1/project_data/"
# -

get_ipython().run_line_magic(
    "spark", "add -s {0}-final_project -l python -u {1} -k".format(username, server)
)

# + language="spark"
# ## SPARK IMPORTS
# from functools import reduce
# from pyspark.sql.functions import col, lit, unix_timestamp, from_unixtime, collect_list
# from pyspark.sql.functions import countDistinct, concat
# from pyspark.sql.functions import udf, explode, split
# from pyspark.sql.types import ArrayType, StringType
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
# finalCols = ["trip_id", "arrival_time_complete", "departure_time_complete", "arrival_time_complete_unix", "departure_time_complete_unix", "stop_id", "stop_sequence"]
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
# stopt = timetable
#
#
#

# + language="spark"
# # adding stationName
# stations = stations.withColumn("station_id", stations.STOP_ID).drop("STOP_ID")
# timetableRouteArrival = timetable.join(stations, stations.station_id == timetable.stop_id)
# timetableRouteArrival = timetableRouteArrival.withColumn("route_stop_id", concat(col("route_id"), lit("$"), col("STOP_NAME")))
# stopt = timetableRouteArrival
# timetableRouteArrival = timetableRouteArrival.select(["route_stop_id", "arrival_time_complete_unix", "departure_time_complete_unix", "route_id", "stop_id", "stop_sequence"])
#
# timetableRouteArrival = timetableRouteArrival.dropDuplicates().cache()
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
# allTimetableRouteArrival.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "timetable.csv")
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
# routeStopsSimple = timetables.select(["end_route_stop_id", "stop_id"]).dropDuplicates()
#
# stationsDB = routeStopsSimple.join(stations, stations.station_id == routeStops.stop_id)\
#         .groupBy(["station_Id", "STOP_NAME", "STOP_LAT", "STOP_LON"])\
#         .agg(collect_list("end_route_stop_id")).cache()
# stationsDB.show()


# + language="spark"
# stationsDB.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "stations.csv")

# + magic_args="-o stationsDB" language="spark"
# stationsDB
# -

stationsDB.to_csv("../data/stations.csv")

# ## Building Stops 
#
#
#
# /!\ Simply doesn't work that way :
#
# We actually need the trip of all the `time_stops` to match the consecutive stops properly.
#
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
# + language="spark"
# routeStops.show(5)

# + language="spark"
# finalCols = ["STOP_NAME", "end_route_stop_id", "route_stop_id", "departure_time_complete_unix", "arrival_time_complete_unix", "stop_sequence", "route_id", "stop_sequence", "end"]
# routeStops = routeStops.join(stations, stations.station_id == routeStops.stop_id, "left").select(finalCols)

# + language="spark"
# ## show the schema
# routeStops.sort("end_route_stop_id", "stop_sequence").show()

# + language="spark"
# routeStops.drop("departure_time_complete_unix").drop("arrival_time_complete_unix").dropDuplicates().count()

# + language="spark"
# arrivals = routeStops.filter(routeStops.end == "A").drop("departure_time_complete_unix").withColumnRenamed("arrival_time_complete_unix", "reached_time")
# departures = routeStops.filter(routeStops.end == "D").drop("arrival_time_complete_unix").withColumnRenamed("departure_time_complete_unix", "reached_time")
# print(arrivals.count())
# print(departures.count())

# + language="spark"
# arrivals.show(5)
# departures.show(5)

# + language="spark"
# route_stops_cols = ["STOP_NAME", "route_id", "end_route_stop_id", "route_stop_id", "target_end_route_stop_id", "travel_time", "stop_sequence"]

# + language="spark"
# ## points to the matching end
# arrivals = arrivals.withColumn("matching_arrival", col("stop_sequence") - 1)
# targets = departures.select(["end_route_stop_id", "stop_sequence", "route_stop_id", "reached_time"])
# targets = targets.withColumnRenamed("end_route_stop_id", "target_end_route_stop_id")
# targets = targets.withColumnRenamed("stop_sequence", "target_sequence")
# targets = targets.withColumnRenamed("route_stop_id", "target_route_stop_id")
# targets = targets.withColumnRenamed("reached_time", "reached_time_target")
# sel_cols = ["STOP_NAME", "stop_sequence", "route_id", "end_route_stop_id", "route_stop_id", "target_end_route_stop_id"]
#
#
# terminus = arrivals.filter("matching_arrival == -1")\
#             .select(sel_cols)\
#             .dropDuplicates()\
#             .cache()
# arrivals = arrivals.join(targets, (targets.target_route_stop_id == departures.route_stop_id) & (targets.target_sequence == arrival.matching_arrival))\
#             .select(sel_cols + ["reached_time_target"])\
#             .dropDuplicates()\
#             .cache()

# + language="spark"
# departures = departures.withColumn("travel_time", col("next_arrival_time") - col("departure_time_complete_unix"))
# departures = departures.select(route_stops_cols)\
#             .dropDuplicates().cache()
# print(departures.count())
# departures.show(5)

# + language="spark"
# arrivals = arrivals.withColumn("travel_time", col("departure_time_complete_unix") - col("arrival_time_complete_unix"))
# arrivals = arrivals.withColumn("target_end_route_stop_id", \
#                                concat(split(col("end_route_stop_id"), "$")[0],split(col("end_route_stop_id"), "$")[1], lit("$D")))
# arrivals = arrivals.select(route_stops_cols).dropDuplicates().cache()
# arrivals.show(5)

# + language="spark"
# print("Route stop count : ")
# print(routeStops.count())
# print("Arrival count :")
# print(arrivals.count())
# print("Departure count :")
# print(departures.count())
# #assert(routeStops.count() == arrivals.count() + departures.count())
# + language="spark"
# print(arrivals.select("end_route_stop_id").count())
# print(targets.select("target_end_route_stop_id").count())
# print(departures.withColumn("matching_arrival", col("stop_sequence") + 1).join(targets, (targets.target_route_stop_id == departures.route_stop_id) & (targets.target_sequence == departures.matching_arrival))\
#             .select(["STOP_NAME", "stop_sequence", "departure_time_complete_unix", "route_id", "end_route_stop_id", "route_stop_id", "target_end_route_stop_id", "next_arrival_time"])\
#             .dropDuplicates().count())


# + language="spark"
# routeStops.select("end_route_stop_id").count()
# -

# ## Builing Stops

# + language="spark"
#
# stopTrips = stopt.select(["trip_id", "arrival_time_complete_unix", "departure_time_complete_unix", "stop_id", 'stop_sequence', "route_id", "STOP_NAME", "route_stop_id"])
# duplicate_marking_function = udf(lambda n : ["A", "D"], ArrayType(StringType()))
# stopTrips = stopTrips.withColumn("duplicate_mark", duplicate_marking_function(stopTrips.route_id))
# stopTrips = stopTrips.withColumn("end", explode(stopTrips.duplicate_mark)).drop("duplicate_mark")
# stopTrips = stopTrips.withColumn("end_route_stop_id", concat(col("route_stop_id"), lit("$"), col("end"))).dropDuplicates().cache()
# stopTrips.cache()

# + language="spark"
# arrivals = stopTrips.filter(stopTrips.end == "A").drop("departure_time_complete_unix").withColumnRenamed("arrival_time_complete_unix", "reached_time")
# departures = stopTrips.filter(stopTrips.end == "D").drop("arrival_time_complete_unix").withColumnRenamed("departure_time_complete_unix", "reached_time")

# + language="spark"
# targets.show(5)

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
# sel_cols = ["STOP_NAME", "stop_sequence", "route_id", "end_route_stop_id", "route_stop_id", "end_route_stop_id", "trip_id"]
#
#
# terminus = arrivals.filter("matching_arrival == -1")\
#             .select(sel_cols)\
#             .dropDuplicates()\
#             .cache()
#
#
# arrivals = arrivals.join(targets, (targets.target_route_stop_id == arrivals.route_stop_id) \
#                          & (targets.target_sequence == arrivals.matching_arrival)\
#                          & (targets.target_trip_id == arrivals.trip_id))\
#             .select(sel_cols + ["reached_time_target"])\
#             .dropDuplicates()\
#             .cache()
# -

x = """cannot resolve '`target_end_route_stop_id`' given input columns: [trip_id, route_id, matching_arrival, end, reached_time, stop_id, stop_sequence, STOP_NAME, route_stop_id, end_route_stop_id];;\n'Project [STOP_NAME#5269, stop_sequence#5334, route_id#5896, end_route_stop_id#6817, route_stop_id#6091, 'target_end_route_stop_id, trip_id#5330]\n+- Filter (matching_arrival#7141 = -1)\n   +- Project [trip_id#5330, reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817, (cast(stop_sequence#5334 as int) - 1) AS matching_arrival#7141]\n      +- Project [trip_id#5330, reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817, (cast(stop_sequence#5334 as int) - 1) AS matching_arrival#7100]\n         +- Project [trip_id#5330, reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817, (cast(stop_sequence#5334 as int) - 1) AS matching_arrival#6992]\n            +- Project [trip_id#5330, arrival_time_complete_unix#5507L AS reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817]\n               +- Project [trip_id#5330, arrival_time_complete_unix#5507L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817]\n                  +- Filter (end#6797 = A)\n                     +- Deduplicate [departure_time_complete_unix#5521L, trip_id#5330, stop_sequence#5334, route_stop_id#6091, route_id#5896, end#6797, STOP_NAME#5269, stop_id#5333, end_route_stop_id#6817, arrival_time_complete_unix#5507L]\n                        +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, concat(route_stop_id#6091, $, end#6797) AS end_route_stop_id#6817]\n                           +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797]\n                              +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, duplicate_mark#6786, end#6797]\n                                 +- Generate explode(duplicate_mark#6786), false, [end#6797]\n                                    +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, <lambda>(route_id#5896) AS duplicate_mark#6786]\n                                       +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091]\n                                          +- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, ms_diff#5798L, route_id#5896, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, PARENT_STATION#5297, station_id#6032, concat(route_id#5896, $, STOP_NAME#5269) AS route_stop_id#6091]\n                                             +- Join Inner, (station_id#6032 = stop_id#5333)\n                                                :- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, ms_diff#5798L, route_id#5896]\n                                                :  +- Join Inner, (trip_id#5330 = trip_id#5898)\n                                                :     :- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, (departure_time_complete_unix#5521L - arrival_time_complete_unix#5507L) AS ms_diff#5798L]\n                                                :     :  +- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334]\n                                                :     :     +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, unix_timestamp(departure_time_complete#5494, yyyy/MM/dd HH:mm:ss, Some(Europe/Zurich)) AS departure_time_complete_unix#5521L]\n                                                :     :        +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, arrival_time_complete#5482, departure_time_complete#5494, unix_timestamp(arrival_time_complete#5482, yyyy/MM/dd HH:mm:ss, Some(Europe/Zurich)) AS arrival_time_complete_unix#5507L]\n                                                :     :           +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, arrival_time_complete#5482, concat(cast(year#5337 as string), /, cast(month#5338 as string), /, cast(day#5339 as string),  , departure_time#5332) AS departure_time_complete#5494]\n                                                :     :              +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, concat(cast(year#5337 as string), /, cast(month#5338 as string), /, cast(day#5339 as string),  , arrival_time#5331) AS arrival_time_complete#5482]\n                                                :     :                 +- Join Inner, (_c0#5360 = stop_id#5333)\n                                                :     :                    :- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339]\n                                                :     :                    :  +- Filter (day#5339 < 17)\n                                                :     :                    :     +- Filter (day#5339 >= 13)\n                                                :     :                    :        +- Filter (month#5338 = 5)\n                                                :     :                    :           +- Filter (year#5337 = 2020)\n                                                :     :                    :              +- Relation[trip_id#5330,arrival_time#5331,departure_time#5332,stop_id#5333,stop_sequence#5334,pickup_type#5335,drop_off_type#5336,year#5337,month#5338,day#5339] orc\n                                                :     :                    +- Relation[_c0#5360] csv\n                                                :     +- Project [route_id#5896, trip_id#5898]\n                                                :        +- Relation[route_id#5896,service_id#5897,trip_id#5898,trip_headsign#5899,trip_short_name#5900,direction_id#5901,year#5902,month#5903,day#5904] orc\n                                                +- Project [STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, PARENT_STATION#5297, station_id#6032]\n                                                   +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, PARENT_STATION#5297, STOP_ID#5262 AS station_id#6032]\n                                                      +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, _c5#5255 AS PARENT_STATION#5297]\n                                                         +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, _c4#5254 AS LOCATION_TYPE#5290, _c5#5255]\n                                                            +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, _c3#5253 AS STOP_LON#5283, _c4#5254, _c5#5255]\n                                                               +- Project [STOP_ID#5262, STOP_NAME#5269, _c2#5252 AS STOP_LAT#5276, _c3#5253, _c4#5254, _c5#5255]\n                                                                  +- Project [STOP_ID#5262, _c1#5251 AS STOP_NAME#5269, _c2#5252, _c3#5253, _c4#5254, _c5#5255]\n                                                                     +- Project [_c0#5250 AS STOP_ID#5262, _c1#5251, _c2#5252, _c3#5253, _c4#5254, _c5#5255]\n                                                                        +- Relation[_c0#5250,_c1#5251,_c2#5252,_c3#5253,_c4#5254,_c5#5255] csv\n"
Traceback (most recent call last):
  File "/hdata/sdb/hadoop/yarn/local/usercache/eric/appcache/application_1652960972356_1913/container_e05_1652960972356_1913_01_000001/pyspark.zip/pyspark/sql/dataframe.py", line 1202, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/hdata/sdb/hadoop/yarn/local/usercache/eric/appcache/application_1652960972356_1913/container_e05_1652960972356_1913_01_000001/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
    answer, self.gateway_client, self.target_id, self.name)
  File "/hdata/sdb/hadoop/yarn/local/usercache/eric/appcache/application_1652960972356_1913/container_e05_1652960972356_1913_01_000001/pyspark.zip/pyspark/sql/utils.py", line 69, in deco
    raise AnalysisException(s.split(': ', 1)[1], stackTrace)
AnalysisException: u"cannot resolve '`target_end_route_stop_id`' given input columns: [trip_id, route_id, matching_arrival, end, reached_time, stop_id, stop_sequence, STOP_NAME, route_stop_id, end_route_stop_id];;\n'Project [STOP_NAME#5269, stop_sequence#5334, route_id#5896, end_route_stop_id#6817, route_stop_id#6091, 'target_end_route_stop_id, trip_id#5330]\n+- Filter (matching_arrival#7141 = -1)\n   +- Project [trip_id#5330, reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817, (cast(stop_sequence#5334 as int) - 1) AS matching_arrival#7141]\n      +- Project [trip_id#5330, reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817, (cast(stop_sequence#5334 as int) - 1) AS matching_arrival#7100]\n         +- Project [trip_id#5330, reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817, (cast(stop_sequence#5334 as int) - 1) AS matching_arrival#6992]\n            +- Project [trip_id#5330, arrival_time_complete_unix#5507L AS reached_time#6962L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817]\n               +- Project [trip_id#5330, arrival_time_complete_unix#5507L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, end_route_stop_id#6817]\n                  +- Filter (end#6797 = A)\n                     +- Deduplicate [departure_time_complete_unix#5521L, trip_id#5330, stop_sequence#5334, route_stop_id#6091, route_id#5896, end#6797, STOP_NAME#5269, stop_id#5333, end_route_stop_id#6817, arrival_time_complete_unix#5507L]\n                        +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797, concat(route_stop_id#6091, $, end#6797) AS end_route_stop_id#6817]\n                           +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, end#6797]\n                              +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, duplicate_mark#6786, end#6797]\n                                 +- Generate explode(duplicate_mark#6786), false, [end#6797]\n                                    +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091, <lambda>(route_id#5896) AS duplicate_mark#6786]\n                                       +- Project [trip_id#5330, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, route_id#5896, STOP_NAME#5269, route_stop_id#6091]\n                                          +- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, ms_diff#5798L, route_id#5896, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, PARENT_STATION#5297, station_id#6032, concat(route_id#5896, $, STOP_NAME#5269) AS route_stop_id#6091]\n                                             +- Join Inner, (station_id#6032 = stop_id#5333)\n                                                :- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, ms_diff#5798L, route_id#5896]\n                                                :  +- Join Inner, (trip_id#5330 = trip_id#5898)\n                                                :     :- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334, (departure_time_complete_unix#5521L - arrival_time_complete_unix#5507L) AS ms_diff#5798L]\n                                                :     :  +- Project [trip_id#5330, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, departure_time_complete_unix#5521L, stop_id#5333, stop_sequence#5334]\n                                                :     :     +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, arrival_time_complete#5482, departure_time_complete#5494, arrival_time_complete_unix#5507L, unix_timestamp(departure_time_complete#5494, yyyy/MM/dd HH:mm:ss, Some(Europe/Zurich)) AS departure_time_complete_unix#5521L]\n                                                :     :        +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, arrival_time_complete#5482, departure_time_complete#5494, unix_timestamp(arrival_time_complete#5482, yyyy/MM/dd HH:mm:ss, Some(Europe/Zurich)) AS arrival_time_complete_unix#5507L]\n                                                :     :           +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, arrival_time_complete#5482, concat(cast(year#5337 as string), /, cast(month#5338 as string), /, cast(day#5339 as string),  , departure_time#5332) AS departure_time_complete#5494]\n                                                :     :              +- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339, _c0#5360, concat(cast(year#5337 as string), /, cast(month#5338 as string), /, cast(day#5339 as string),  , arrival_time#5331) AS arrival_time_complete#5482]\n                                                :     :                 +- Join Inner, (_c0#5360 = stop_id#5333)\n                                                :     :                    :- Project [trip_id#5330, arrival_time#5331, departure_time#5332, stop_id#5333, stop_sequence#5334, drop_off_type#5336, year#5337, month#5338, day#5339]\n                                                :     :                    :  +- Filter (day#5339 < 17)\n                                                :     :                    :     +- Filter (day#5339 >= 13)\n                                                :     :                    :        +- Filter (month#5338 = 5)\n                                                :     :                    :           +- Filter (year#5337 = 2020)\n                                                :     :                    :              +- Relation[trip_id#5330,arrival_time#5331,departure_time#5332,stop_id#5333,stop_sequence#5334,pickup_type#5335,drop_off_type#5336,year#5337,month#5338,day#5339] orc\n                                                :     :                    +- Relation[_c0#5360] csv\n                                                :     +- Project [route_id#5896, trip_id#5898]\n                                                :        +- Relation[route_id#5896,service_id#5897,trip_id#5898,trip_headsign#5899,trip_short_name#5900,direction_id#5901,year#5902,month#5903,day#5904] orc\n                                                +- Project [STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, PARENT_STATION#5297, station_id#6032]\n                                                   +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, PARENT_STATION#5297, STOP_ID#5262 AS station_id#6032]\n                                                      +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, LOCATION_TYPE#5290, _c5#5255 AS PARENT_STATION#5297]\n                                                         +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, STOP_LON#5283, _c4#5254 AS LOCATION_TYPE#5290, _c5#5255]\n                                                            +- Project [STOP_ID#5262, STOP_NAME#5269, STOP_LAT#5276, _c3#5253 AS STOP_LON#5283, _c4#5254, _c5#5255]\n                                                               +- Project [STOP_ID#5262, STOP_NAME#5269, _c2#5252 AS STOP_LAT#5276, _c3#5253, _c4#5254, _c5#5255]\n                                                                  +- Project [STOP_ID#5262, _c1#5251 AS STOP_NAME#5269, _c2#5252, _c3#5253, _c4#5254, _c5#5255]\n                                                                     +- Project [_c0#5250 AS STOP_ID#5262, _c1#5251, _c2#5252, _c3#5253, _c4#5254, _c5#5255]\n                                                                        +- Relation[_c0#5250,_c1#5251,_c2#5252,_c3#5253,_c4#5254,_c5#5255] csv\n"""
print(x)

print("y")


