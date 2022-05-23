# # Create Spark Session

import os
# %load_ext sparkmagic.magics
username = os.environ['RENKU_USERNAME']
server = 'http://iccluster044.iccluster.epfl.ch:10000'
from IPython import get_ipython
get_ipython().run_cell_magic('spark', line="config", 
                             cell="""{{ "name":"{0}-final_project", "executorMemory":"4G", "executorCores":4, "numExecutors":10 }}""".format(username))

get_ipython().run_line_magic(
    "spark", "add -s {0}-final_project -l python -u {1} -k".format(username, server)
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

# + language="spark"
# stations = spark.read.csv("/data/sbb/csv/allstops/stop_locations.csv")

# + language="spark"
# from functools import reduce
#
# oldColumns = stations.schema.names
# newColumns = ["STOP_ID", "STOP_NAME", "STOP_LAT", "STOP_LON", "LOCATION_TYPE", "PARENT_STATION"]
#
# stations = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), stations)
# stations.printSchema()
# stations.show()
# -
# ## Building timetables


# + language="spark"
# stopt = spark.read.orc("/data/sbb/part_orc/stop_times")
# sel_stops = spark.read.csv("/user/benhaim/final-project/stop_ids_in_radius.csv")
# stopt.show(5)
# sel_stops.show(5)
# -
# Few numbers :


# + language="spark"
# from pyspark.sql.functions import countDistinct, concat
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

# ## We select the stops that are withing the range 

# + language="spark"
# stopt = stopt.select(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "drop_off_type","year", "month", "day"])
# stopt.show()
# -

# ## We select the stops within range

# + language="spark"
# stopt = stopt.join(sel_stops, sel_stops._c0 == stopt.stop_id, "inner")

# + language="spark"
# from pyspark.sql.functions import col,lit
# stopt = stopt.withColumn("arrival_time_complete", \
#                          concat(col("year"), lit("/"), col("month"), lit("/"), col("day"), lit(" "), col("arrival_time")))
# stopt = stopt.withColumn("departure_time_complete", \
#                          concat(col("year"), lit("/"), col("month"), lit("/"), col("day"), lit(" "), col("departure_time")))
# stopt = stopt.cache()
#
# stopt.select(["arrival_time_complete", "departure_time_complete"]).show()

# + language="spark"
# from datetime import datetime
# from pyspark.sql.functions import unix_timestamp, from_unixtime
#
# ## computing spark time 
# stopt = stopt.withColumn('arrival_time_complete_unix', unix_timestamp('arrival_time_complete', "yyyy/MM/dd HH:mm:ss"))
# stopt = stopt.withColumn('departure_time_complete_unix', unix_timestamp('departure_time_complete', "yyyy/MM/dd HH:mm:ss"))

# + language="spark"
# stopt.show()

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

# ## Selection of stops within the right radius

# ## Get route id

# + language="spark"
# spark.read.orc("/data/sbb/part_orc/trips").show()

# + language="spark"
# ## keep arrival
# trips = spark.read.orc("/data/sbb/part_orc/trips").select(["route_id", "trip_id"])
# trips.show(5)

# + language="spark"
# stopt.count()

# + language="spark"
# timetable = stopt.join(trips, "trip_id", "inner")

# + language="spark"
# timetable = timetable.withColumn("route_stop_id", concat(col("route_id"), lit("-"), col("stop_id"))).select(["route_stop_id", "arrival_time_complete_unix", "route_id", "stop_id"])
# timetable.show(10)

# + language="spark"
# timetableRouteArrival = timetable.dropDuplicates().cache()
# timetableRouteArrival.show()

# + language="spark"
# timetableRouteArrival.write.csv("/user/magron/final_project/timetableRouteArrivalFilteredFinal.csv")
# -

# ## Construction of station

# + language="spark"
# stations.show()

# + language="spark"
#
# stations = stations.join(sel_stops, sel_stops._c0 == stations.STOP_ID, "inner").cache()
# print(stations.count())
# stations.show()

# + magic_args="-o timetableRouteArrival -n -1" language="spark"
# timetableRouteArrival

# + language="spark"
# stations.show()

# + language="spark"
#
# timetableRouteArrival = stations.join(timetableRouteArrival, timetableRouteArrival.stop_id == stations.STOP_ID).select(["STOP_NAME", "arrival_time_complete_unix", "route_id"])
# timetableRouteArrival = timetableRouteArrival.withColumn("route_stop_id", concat(col("route_id"), lit("-"), col("STOP_NAME"))).select(["route_stop_id", "arrival_time_complete_unix", "route_id"])

# + language="spark"
# stoptimes = spark.read.orc("/data/sbb/part_orc/stop_times")
# stoptimes.show()
# -


