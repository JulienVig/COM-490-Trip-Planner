# ### New preprocessing

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
# from pyspark.sql.window import Window
# REMOTE_PATH = "/group/abiskop1/project_data/"

# + language="spark"
#
# def count_nan_null(df):
#     df.select([F.count(F.when(F.isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()
#
#
# def read_orc(fname):
#     df = spark.read.orc("/data/sbb/part_orc/{name}".format(name=fname))
#     return df.filter((df.year == 2020) & (df.month == 5) & (df.day > 12) & (df.day < 18))
#
#
# def write_hdfs(df, dirname):
#     df.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + dirname)

# + language="spark"
# spark.conf.set("spark.sql.session.timeZone", "UTC+2")
# -

# ## Loading selected stations (Stops)

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
# ## Stops in radius


# + language="spark"
# sel_stops = spark.read.csv("/user/benhaim/final-project/stop_ids_in_radius.csv")
# sel_stops = sel_stops.withColumnRenamed("_c0", "stop_id")
# sel_stops.show(5)
# -
# ## Stop times

# + language="spark"
# stoptimes = spark.read.orc("/data/sbb/part_orc/stop_times")
# stoptimes.printSchema()
# -

# ### Stop times at relevant date

# + language="spark"
# relevant_stoptimes = stoptimes.filter("year == 2020").filter("month == 5").filter("day == 13")
# -

# ### Stop times in radius

# + language="spark"
# close_stoptimes = relevant_stoptimes.join(sel_stops, on="stop_id",how="inner")
#
# close_stoptimes = close_stoptimes.withColumn("arrival_time_complete", \
#                          concat(col("year"), lit("/"), col("month"), lit("/"), col("day"), lit(" "), col("arrival_time")))
# close_stoptimes = close_stoptimes.withColumn('arrival_time', unix_timestamp('arrival_time_complete', "yyyy/MM/dd HH:mm:ss")).dropna()
# close_stoptimes = close_stoptimes.cache()
# close_stoptimes.printSchema()
# -

# # TODO check mean of count() which is the mean number of stops for one trip

# ## Trips

# + language="spark"
# trips = read_orc("trips")
# trips.show()
# -

# ### Checking assumption : pair (trip_id, route_id) is unique

# + language="spark"
# ispairunique = trips.select("route_id", "trip_id")
# print(ispairunique.count() == ispairunique.dropDuplicates().count())
# -

# ### Merging trips and stop_times

# + language="spark"
# selected_stoptimes = close_stoptimes.select("trip_id", "stop_id", "arrival_time", "stop_sequence")
# trips_stop_times = trips.select("route_id", "trip_id", "trip_headsign").join(selected_stoptimes, on="trip_id",how="inner")
# trips_stop_times = trips_stop_times.withColumn("route_stop_id", concat(col("route_id"), lit("&"), col("stop_id")))
# trips_stop_times.count()
# -

# ### Building time tables

# + language="spark"
# timetable = trips_stop_times.select(["route_stop_id", "arrival_time"])

# + language="spark"
# write_hdfs(timetable, "timetableRefacFinal")
# -

# ### Building Route stops

# + language="spark"
#
# max_stop_times = trips_stop_times.sort(F.desc("stop_sequence")).dropDuplicates(["route_id"])
# max_stop_times = max_stop_times.select('trip_id')
# max_stop_times.show(5)
# max_stop_times.count()

# + language="spark"
# actual_routes = trips_stop_times.join(max_stop_times, "trip_id", "inner")
# actual_routes.count()

# + language="spark"
#
# w = Window.partitionBy("route_id").orderBy(col("stop_sequence").desc())
# route_stops = actual_routes.withColumn("actual_stop_seq", F.row_number().over(w)).drop("trip_id", "stop_sequence")
# print(actual_routes.count())
# print(route_stops.count())
#
# prevs = route_stops.drop("trip_headsign", "stop_id")\
#                   .withColumnRenamed("actual_stop_seq", "prev_stop_seq")\
#                   .withColumnRenamed("route_stop_id", "prev_route_stop_id")\
#                   .withColumnRenamed("arrival_time", "prev_arrival_time")\
#                   .withColumnRenamed("route_id", "prev_route_id")                    
#
# route_stops = route_stops.withColumn("matching_stop_seq", col("actual_stop_seq") + 1)
#
# route_stops = route_stops.join(prevs, (prevs.prev_stop_seq == route_stops.matching_stop_seq) \
#                                       & (prevs.prev_route_id == route_stops.route_id), "leftouter")\
#                          .cache()
# print(route_stops.count())

# + language="spark"
# complete_route_stops = route_stops.withColumn("travel_time", col("arrival_time") - col("prev_arrival_time"))\
#                         .drop("prev_stop_seq", "prev_arrival_time", "arrival_time", 'matching_stop_seq', 'prev_route_id')\
#                         .cache()
# complete_route_stops.show(5)

# + language="spark"
# routes_orc = read_orc("routes").select('route_id', 'route_desc', 'route_short_name')
# final_complete_route_stops = complete_route_stops.join(routes_orc, 'route_id', 'inner')\
#                                                 .drop('route_id')\
#                                                 .join(stations.select('stop_id', 'stop_name'), 'stop_id', 'inner')
#
# final_complete_route_stops.show(5)
# -

# # Stations

# + language="spark"
# final_stations = final_complete_route_stops.groupby('stop_id')\
#     .agg(
#         F.collect_set(col('route_stop_id')).alias('route_stops')
#     ).join(stations, 'stop_id', 'inner').drop('location_type', 'parent_station')
# final_stations.show(5, False)
# -


