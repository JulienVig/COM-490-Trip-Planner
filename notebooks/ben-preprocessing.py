# +
import os
import pandas as pd
pd.set_option("display.max_columns", 50)
import matplotlib.pyplot as plt
# %matplotlib inline
import plotly.express as px
import plotly.graph_objects as go
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

username = os.environ['RENKU_USERNAME']
hiveaddr = os.environ['HIVE_SERVER2']
(hivehost,hiveport) = hiveaddr.split(':')
print("Operating as: {0}".format(username))

# +
from pyhive import hive

# create connection
conn = hive.connect(host=hivehost, 
                    port=hiveport,
                    username=username) 
# create cursor
cur = conn.cursor()
# -

### Create your database if it does not exist
query = """
CREATE DATABASE IF NOT EXISTS {0} LOCATION '/user/{0}/hive'
""".format(username)
cur.execute(query)

### Make your database the default
query = """
USE {0}
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.istdaten LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Spark

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
