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
#     df.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite')\
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
#
# w = Window.partitionBy('STOP_NAME').orderBy(col("STOP_ID").asc())
# station_id_translation_table = stations.withColumn("row_number",  F.row_number().over(w))\
#                                         .filter(col('row_number') == 1).drop('row_number')\
#                                         .select(['stop_name', 'stop_id'])
#
# stations = stations.withColumnRenamed('stop_id', 'old_stop_id').join(station_id_translation_table, 'stop_name', 'inner')
# stations.orderBy('stop_name').show(10, False)
# + language="spark"
# stations.filter(col('stop_id') == 8579991).show()
# merged_stoptimes.filter(col('stop_id') == 8579991).show()
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
# # drop hours above 24
# close_stoptimes = close_stoptimes.withColumn('arrival_time', 
#                                              unix_timestamp('arrival_time_complete', "yyyy/MM/dd HH:mm:ss")).dropna()
# close_stoptimes = close_stoptimes.cache()
# close_stoptimes.printSchema()

# + language="spark"
# close_stoptimes.count()

# + language="spark"
# merged_stoptimes = close_stoptimes.withColumnRenamed('stop_id', 'old_stop_id')\
#                                     .join(stations.drop('stop_name', 'stop_lat', 'stop_lon', 'location_type', 'parent_station'), 'old_stop_id')
# merged_stoptimes.count()
# -



# As we can see, in a single day arrival times are duplicated for each stop_id, we will therefore drop them

# + language="spark"
# print(merged_stoptimes.count())
# merged_stoptimes.dropDuplicates(['stop_id','arrival_time']).count()
# -

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
# Create clean_stop_seq such that stop sequence are successive

# + language="spark"
# selected_stoptimes = merged_stoptimes.select("trip_id", "stop_id", "departure_time", "arrival_time", "stop_sequence")
# trips_stop_times = trips.select("route_id", "trip_id", "trip_headsign", 'direction_id').join(selected_stoptimes, on="trip_id",how="inner")
# trips_stop_times = trips_stop_times.withColumnRenamed("route_id", "short_route_id")
# trips_stop_times = trips_stop_times.withColumn("route_id", concat(col("short_route_id"), lit("+"), col("direction_id"), lit('+'), col("trip_headsign")))
#
# w = Window.partitionBy(['trip_id']).orderBy(col("stop_sequence").asc())
# trips_stop_times = trips_stop_times.withColumn("clean_stop_seq", F.row_number().over(w))
# trips_stop_times.count()

# + language="spark"
# trips_stop_times.filter(col("clean_stop_seq") != col('stop_sequence')).dropDuplicates(["trip_id"]).count()
# -

# Some routes loop over the same stops, therefore we add the occurence index in the stop id for each trip to create
# trip_stop_id and route_stop_id.

# + language="spark"
# w = Window.partitionBy(['trip_id', 'stop_id']).orderBy(col("clean_stop_seq").desc())
# stop_times_ranked = trips_stop_times.withColumn("trip_stop_index", F.row_number().over(w))\
#                                     .withColumn("trip_stop_id", concat(col("stop_id"), lit("*"), col("trip_stop_index")))\
#                                     .withColumn("route_stop_id", concat(col("route_id"), lit("&"), col("trip_stop_id")))\
#                                     .orderBy(['route_stop_id', 'trip_id', 'clean_stop_seq']).cache()
# stop_times_ranked.count()

# + language="spark"
# ispairunique = stop_times_ranked.select("trip_stop_id", "trip_id")
# print(ispairunique.count() == ispairunique.dropDuplicates().count())

# + magic_args="   " language="spark"
# cols = ["route_stop_id", "arrival_time"]
# duplicates = stop_times_ranked.join(
#     stop_times_ranked.groupBy(cols).agg((F.count("*")>1).cast("int").alias("Duplicate_indicator")),
#     on=cols,
#     how="inner"
# ).cache()

# + language="spark"
# duplicates.filter(col("Duplicate_indicator") > 0).count()

# + language="spark"
# routes_orc = read_orc("routes").select('route_id', 'route_desc', 'route_short_name').withColumnRenamed('route_id', 'short_route_id')
#
# duplicates.filter(col("Duplicate_indicator") > 0).join(routes_orc, 'short_route_id', 'inner')\
#             .join(stations.select('stop_id', 'stop_name'), 'stop_id', 'inner').orderBy(cols).show()
# -

# ### Building time tables
#
# We drop duplicated arrival times for the same (route, stop)

# + language="spark"
# timetable = stop_times_ranked.select(["route_stop_id", "arrival_time"])\
#                     .dropDuplicates(["route_stop_id", "arrival_time"]).cache()
# timetable.count()

# + language="spark"
# write_hdfs(timetable, "timetableRefacFinal")
# -

# ### Building Route stops

# + language="spark"
#
# window = Window.partitionBy("trip_id").orderBy(col("clean_stop_seq").desc())
#
# max_stop_times = stop_times_ranked.withColumn("row",F.row_number().over(window)) \
#   .filter(col("row") == 1).drop("row").dropDuplicates(["route_id"])
#
# max_stop_times = max_stop_times.select(['trip_id', 'clean_stop_seq']).cache()
# max_stop_times.show(5)
# max_stop_times.count()

# + language="spark"
# max_stop_times.select('clean_stop_seq').groupBy().sum().show()

# + language="spark"
# actual_routes = stop_times_ranked.join(max_stop_times.select('trip_id'), "trip_id", "inner").cache()
# actual_routes.count()

# + language="spark"
# print(actual_routes.count())
# actual_routes.dropDuplicates(['route_stop_id']).count()

# + language="spark"
#
# w = Window.partitionBy("route_id").orderBy(col("clean_stop_seq").desc())
# route_stops = actual_routes.withColumn("actual_stop_seq", F.row_number().over(w)).drop("trip_id", "clean_stop_seq")
# print(actual_routes.count())
# print(route_stops.count())
#
# prevs = route_stops.drop("trip_headsign", "stop_id")\
#                   .withColumnRenamed("actual_stop_seq", "prev_stop_seq")\
#                   .withColumnRenamed("route_stop_id", "prev_route_stop_id")\
#                   .withColumnRenamed("arrival_time", "prev_arrival_time")\
#                   .withColumnRenamed("route_id", "prev_route_id")\
#                     .select(['prev_stop_seq', 'prev_route_stop_id', 
#                              'prev_arrival_time', 'prev_route_id'])
#
# route_stops = route_stops.withColumn("matching_stop_seq", col("actual_stop_seq") + 1)
#
#
# route_stops = route_stops.join(prevs, (prevs.prev_stop_seq == route_stops.matching_stop_seq) \
#                                       & (prevs.prev_route_id == route_stops.route_id), "leftouter").cache()
#
# print(route_stops.count())

# + language="spark"
# complete_route_stops = route_stops.withColumn("travel_time", col("arrival_time") - col("prev_arrival_time"))\
#                         .drop("prev_stop_seq", "prev_arrival_time", "arrival_time",
#                               'matching_stop_seq', 'prev_route_id', 'clean_stop_seq'
#                               'short_route_id', 'direction_id', 'departure_time', 
#                               'stop_sequence', 'trip_stop_index', 'trip_stop_id')\
#                         .cache()
# complete_route_stops.show(5, False)

# + language="spark"
# unique_stations = stations.select('stop_id', 'stop_name').dropDuplicates()
# routes_orc = read_orc("routes").select('route_id', 'route_desc', 'route_short_name').withColumnRenamed('route_id', 'short_route_id')
# final_complete_route_stops = complete_route_stops.join(routes_orc, 'short_route_id', 'inner')\
#                                                 .drop('route_id')\
#                                                 .join(unique_stations, 'stop_id', 'inner')
# final_complete_route_stops.count()

# + language="spark"
# print(final_complete_route_stops.count())
# print(complete_route_stops.count())

# + language="spark"
# write_hdfs(final_complete_route_stops, "routestops")

# + language="spark"
# count_nan_null(final_complete_route_stops)
# -

# # Stations

# + language="spark"
# unique_stations = stations.drop('location_type', 'parent_station').dropDuplicates(['stop_id'])
# final_stations = final_complete_route_stops.groupby('stop_id')\
#                                             .agg(F.collect_list(col('route_stop_id')).alias('route_stops'))\
#                                             .join(unique_stations, 'stop_id', 'inner').cache()
# final_stations.show(5, False)
# + magic_args="-o  final_stations" language="spark"
# final_stations.count()
# -

final_stations.to_csv('../data/stations.csv', index=False)


