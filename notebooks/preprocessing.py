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
CREATE DATABASE IF NOT EXISTS {0} LOCATION '/user/{0}/finalproject'
""".format(username)
cur.execute(query)

### Make your database the default
query = """
USE {0}
""".format(username)
cur.execute(query)

# # Istdaten

query = """
DROP TABLE IF EXISTS {0}.istdaten
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.istdaten(
        BETRIEBSTAG string,
        FAHRT_BEZEICHNER string,
        BETREIBER_ID string,
        BETREIBER_ABK string,
        BETREIBER_NAME string,
        PRODUKT_ID string,
        LINIEN_ID string,
        LINIEN_TEXT string,
        UMLAUF_ID string,
        VERKEHRSMITTEL_TEXT string,
        ZUSATZFAHRT_TF string,
        FAELLT_AUS_TF string,
        BPUIC string,
        HALTESTELLEN_NAME string,
        ANKUNFTSZEIT string,
        AN_PROGNOSE string,
        AN_PROGNOSE_STATUS string,
        ABFAHRTSZEIT string,
        AB_PROGNOSE string,
        AB_PROGNOSE_STATUS string,
        DURCHFAHRT_TF string
    )
    PARTITIONED BY (year STRING, month STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/istdaten'
    TBLPROPERTIES ("skip.header.line.count"="1", "orc.compress"="SNAPPY")
""".format(username)
cur.execute(query)

query = """
    MSCK REPAIR TABLE {0}.istdaten
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.istdaten LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Stop times

query = """
DROP TABLE IF EXISTS {0}.stoptimes
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.stoptimes(
        TRIP_ID string,
        ARRIVAL_TIME string,
        DEPARTURE_TIME string,
        STOP_ID string,
        STOP_SEQUENCE string,
        PICKUP_TYPE string,
        DROP_OFF_TYPE string
    )
    PARTITIONED BY (year STRING, month STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/stop_times'
    TBLPROPERTIES ("orc.compress"="SNAPPY")
""".format(username)
cur.execute(query)

query = """
    MSCK REPAIR TABLE {0}.stoptimes
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.stoptimes LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Trips

query = """
DROP TABLE IF EXISTS {0}.trips
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.trips(
        ROUTE_ID string,
        SERVICE_ID string,
        TRIP_ID string,
        TRIP_HEADSIGN string,
        TRIP_SHORT_NAME string,
        DIRECTION_ID string
    )
    PARTITIONED BY (year STRING, month STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/trips'
    TBLPROPERTIES ("orc.compress"="SNAPPY")
""".format(username)
cur.execute(query)

query = """
    MSCK REPAIR TABLE {0}.trips
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.trips LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Calendar

query = """
DROP TABLE IF EXISTS {0}.calendar
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.calendar(
        SERVICE_ID string,
        MONDAY string,
        TUESDAY string,
        WEDNESDAY string,
        THURSDAY string,
        FRIDAY string,
        SATURDAY string,
        SUNDAY string,
        START_DATE string,
        END_DATE string
    )
    PARTITIONED BY (year STRING, month STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/calendar'
    TBLPROPERTIES ("orc.compress"="SNAPPY")
""".format(username)
cur.execute(query)

query = """
    MSCK REPAIR TABLE {0}.calendar
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.calendar LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Routes

query = """
DROP TABLE IF EXISTS {0}.routes
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.routes(
        ROUTE_ID string,
        AGENCY_ID string,
        ROUTE_SHORT_NAME string,
        ROUTE_LONG_NAME string,
        ROUTE_DESC string,
        ROUTE_TYPE string
    )
    PARTITIONED BY (year STRING, month STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/routes'
    TBLPROPERTIES ("orc.compress"="SNAPPY")
""".format(username)
cur.execute(query)

query = """
    MSCK REPAIR TABLE {0}.routes
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.routes LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Transfers

query = """
DROP TABLE IF EXISTS {0}.transfers
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.transfers(
        from_stop_id string,
        to_stop_id string,
        transfer_type string,
        min_transfer_time string
    )
    PARTITIONED BY (year STRING, month STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/transfers'
    TBLPROPERTIES ("orc.compress"="SNAPPY")
""".format(username)
cur.execute(query)

query = """
    MSCK REPAIR TABLE {0}.transfers
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.transfers LIMIT 5
""".format(username)
pd.read_sql(query, conn)

# # Stops

query = """
DROP TABLE IF EXISTS {0}.stops
""".format(username)
cur.execute(query)

query = """
CREATE EXTERNAL TABLE {0}.stops(
        STOP_ID string,
        STOP_NAME string,
        STOP_LAT string,
        STOP_LON string,
        LOCATION_TYPE string,
        PARENT_STATION string
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,'
    STORED AS TEXTFILE
    LOCATION '/data/sbb/csv/allstops'
""".format(username)
cur.execute(query)

query = """
SELECT * FROM {0}.stops LIMIT 10
""".format(username)
pd.read_sql(query, conn)


