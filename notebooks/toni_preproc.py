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
# -


