

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
# + language="spark"
# sel_stops.count()
# -

# ## Stop times

# + language="spark"
# stoptimes = spark.read.orc("/data/sbb/part_orc/stop_times")
# stoptimes.printSchema()
# -

# ### Stop times at relevant date

# + language="spark"
# stoptimes = stoptimes.filter("year == 2020").filter("month == 5").filter("day == 13")
# -

# ### Stop times in radius

# + language="spark"
# stoptimes = stoptimes.join(sel_stops, on="stop_id",how="inner")
# print(stoptimes.count())
# stoptimes.show()

# + language="spark"
# stoptimes.groupBy("trip_id").count().show()
# -

# # TODO check mean of count() which is the mean number of stops for one trip

# ## Trips

# + language="spark"
# trips = spark.read.orc("/data/sbb/part_orc/trips")
# trips.printSchema()
# trips.count()
# -

# ### Trips at relevant date

# + language="spark"
# trips = trips.filter("year == 2020").filter("month == 5").filter("day == 13")
# trips.count()
# -

# ### Checking assumption : pair (trip_id, route_id) is unique

# + language="spark"
# ispairunique = trips.select("route_id", "trip_id")
# ispairunique.count()

# + language="spark"
# ispairunique.dropDuplicates().count()
# -

# ### Merging trips and stop_times

# + language="spark"
# stoptimes = stoptimes.select("trip_id", "stop_id", "arrival_time", "stop_sequence")
# trips_stop_times = trips.select("route_id", "trip_id", "trip_headsign").join(stoptimes, on="trip_id",how="inner")
# trips_stop_times.count()

# + language="spark"
# trips_stop_times.show(5)
