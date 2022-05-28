# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Building the stochastic timetables
#
# ### Creating Spark Session

import os
# %load_ext sparkmagic.magics
from datetime import datetime
username = os.environ['RENKU_USERNAME']
server = "http://iccluster029.iccluster.epfl.ch:8998"
from IPython import get_ipython
get_ipython().run_cell_magic('spark', line="config", 
                             cell="""{{ "name":"{0}-final_project", "executorMemory":"4G", "executorCores":4, "numExecutors":10 }}""".format(username))

get_ipython().run_line_magic(
    "spark", "add -s {0}-final_project -l python -u {1} -k".format(username, server)
)

# + language="spark"
# ## SPARK IMPORTS
# from functools import reduce
# from pyspark.sql.types import ArrayType, StringType
# from pyspark.sql.functions import *
#
# REMOTE_PATH = "/group/abiskop1/project_data/"

# + language="spark"
# real_time = spark.read.orc("/data/sbb/part_orc/istdaten").dropna()
#
# arrivals = spark.read.csv(REMOTE_PATH + "arrivalsRouteStops.csv", header='true', inferSchema='true')
# arrivals = arrivals.withColumn("route_id", udf(lambda end_id : end_id.split("$")[0])(col("end_route_stop_id")))
#
# print("The Schema is :")
# real_time
# -

# As I don't speak german :

# + language="spark"
# mapping =    [['BETRIEBSTAG', 'date'],
#     ['FAHRT_BEZEICHNER', "trip_id"],
#     ['BETREIBER_ABK', 'operator'],
#     ["BETREIBER_NAME", "operator_name"],
#     ["PRODUCT_ID", "type_transport"],
#     ["LINIEN_ID"," for trains, this is the train number"],
#     ["LINIEN_TEXT","type_service_1"], 
#     ["VERKEHRSMITTEL_TEXT","type_service_2"],
#     ["ZUSATZFAHRT_TF","additional_trip"],
#     ["FAELLT_AUS_TF","trip_failed"],
#     ["HALTESTELLEN_NAME","STOP_NAME"],
#     ["ANKUNFTSZEIT","arrival_time_schedule"],
#     ["AN_PROGNOSE","arrival_time_actual"],
#     ["AN_PROGNOSE_STATUS","measure_method_arrival"],
#     ["ABFAHRTSZEIT","departure_time_schedule"],
#     ["AB_PROGNOSE","departure_time_actual"],
#     ["AB_PROGNOSE_STATUS","measure_method_arrival"],
#     ["DURCHFAHRT_TF","does_stop_here"]]
#
#
# for de_name, en_name in mapping:
#     real_time = real_time.withColumnRenamed(de_name, en_name)
#     
# print("Final Schema :")
# real_time
# -

# ### Restricting the station to the selected ones where transports arrive

# + language="spark"
#
# stations = arrivals.select("STOP_NAME").dropDuplicates()
# #print("Before selection real_data size : ", real_time.count())
# real_time = real_time.join(stations, "STOP_NAME")
# #print("After selection real_data size : ", real_time.count())
# -

# ## EDA of the delay distribution

# + language="spark"
# real_time = real_time.withColumn('arrival_time_schedule', unix_timestamp('arrival_time_schedule', "dd.MM.yyyy HH:mm"))
# real_time = real_time.withColumn('arrival_time_actual', unix_timestamp('arrival_time_actual', "dd.MM.yyyy HH:mm"))
# real_time = real_time.withColumn("arrival_delay", col("arrival_time_actual") - col("arrival_time_schedule"))

# + language="spark"
# analysis = real_time.filter("year == 2021").filter("month == 1")
# analysis = analysis.withColumn("arrival_delay", when(analysis["arrival_delay"] < 0, 0).when(col("arrival_delay").isNull(), 0).otherwise(col("arrival_delay")))
# delays_distrib = analysis.select(["STOP_NAME", "produkt_id", "arrival_delay"]).groupBy(["STOP_NAME", "produkt_id","arrival_delay"]).count().cache()
#
# -

# #### test with :
#
# - Schützenmattstrasse : Bus, tram
# - Länggasse
# - Vacallo, Piazza
# - Studen BE

# +
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import expon
import numpy as np
from scipy.optimize import curve_fit

def plot_delay_dist(sample_dist):
    plt.figure(figsize=(20, 6))
    sample_dist = sample_dist.copy().sort_values("arrival_delay")
    station = sample_dist.STOP_NAME.iloc[0]
    transport_mean = sample_dist.produkt_id.iloc[0]
    
    g = sns.barplot(data=sample_dist.sort_values("arrival_delay"), x="arrival_delay", y="count")
    g.set_xticklabels(g.get_xticklabels(), rotation=45)
    popt, pcov = curve_fit(pdf, sample_dist.arrival_delay, sample_dist['count'])
    yy = pdf(sample_dist.arrival_delay, *popt)
    g.plot(range(len(sample_dist)), yy, '-o')
    print("STATION :", station)
    print("MEAN OF TRANSPORTATION : ", transport_mean);


# + magic_args="-o sample_dist" language="spark"
# sample_dist = delays_distrib.filter(delays_distrib.STOP_NAME ==  "Zürich, Hardplatz").filter(delays_distrib.produkt_id == "Bus")

# + language="spark"
# from pyspark.sql.functions import pandas_udf, PandasUDFType
# import numpy as np
# from scipy.optimize import curve_fit
#
# @udf
# def test_udf(l):
#     counts = np.array(l[1])
#     popt, pcov = curve_fit(lambda x, a: a*np.exp(-a*x), l[0], counts /counts.sum(), p0=[0])
#     return float(popt[0])

# + tags=[] language="spark"
# delays_distrib.withColumn("arrival_delay", col("arrival_delay") /60)\
#                 .groupBy(['STOP_NAME', 'produkt_id'])\
#                 .agg(struct(collect_list("arrival_delay"), collect_list("count"), sum("count")).alias("delays"))\
#                 .withColumn("lambda", test_udf(col("delays")))\
#                 .filter(delays_distrib.STOP_NAME ==  "Zürich, Hardplatz").show()
# -

sample_dist['count'].sum()

sample_dist['count'] = sample_dist['count'] / sample_dist['count'].sum()
sample_dist["arrival_delay"] = sample_dist["arrival_delay"] / 60
#sample_dist = sample_dist.sort_values("arrival_delay")

# +
def pdf(x, a):
        return a*np.exp(-a*x)
    
def cdf(x, a):
        return 1 - np.exp(-a*x)
    
popt, pcov = curve_fit(lambda x, a: a*np.exp(-a*x), sample_dist.arrival_delay,  sample_dist['count'], p0=[0])
print(popt)
yy = pdf(sample_dist.arrival_delay.sort_values(), *popt)
plt.plot(sample_dist.arrival_delay.sort_values(), yy, '-o')
# -

cdf(4, popt[0])

plot_delay_dist(sample_dist)

# + magic_args="-o sample_dist" language="spark"
# sample_dist = delays_distrib.filter(delays_distrib.STOP_NAME ==  "Schützenmattstrasse").filter(delays_distrib.produkt_id == "Tram")
# -

plot_delay_dist(sample_dist)

# + magic_args="-o sample_dist" language="spark"
# sample_dist = delays_distrib.filter(delays_distrib.STOP_NAME ==  "Schützenmattstrasse").filter(delays_distrib.produkt_id == "Bus")
# -

plot_delay_dist(sample_dist)

# + magic_args="-o sample_dist" language="spark"
# sample_dist = delays_distrib.filter(delays_distrib.STOP_NAME ==  "Studen BE").filter(delays_distrib.produkt_id == "Zug")
# -

plot_delay_dist(sample_dist)

# + magic_args="-o sample_dist" language="spark"
# sample_dist = delays_distrib.filter(delays_distrib.STOP_NAME ==  "Zürich, Oerlikerhus").filter(delays_distrib.produkt_id == "Tram")
# -

plot_delay_dist(sample_dist)

# + language="spark"
# #delays_distrib.filter(delays_distrib.STOP_NAME ==  "Studen BE").select("produkt_id").dropDuplicates().show()
# delays_distrib.filter(delays_distrib.STOP_NAME == "Zürich, Oerlikerhus").select("produkt_id").dropDuplicates().show(30)

# + magic_args="-o full_dist" language="spark"
# full_dist = delays_distrib.groupby("arrival_delay").sum()
# -

full_dist["arrival_delay"] = full_dist["arrival_delay"] / 60
full_dist = full_dist[full_dist.arrival_delay < 20]
full_dist = full_dist[full_dist.arrival_delay >= 0]

plt.subplots(figsize=(20, 5))
#full_count = full_dist["sum(count)"].sum()
#full_dist["y"] = full_dist["sum(count)"] / full_count
g = sns.barplot(data=full_dist.sort_values("arrival_delay"), x="arrival_delay", y="sum(count)")
g.set_xticklabels(g.get_xticklabels(), rotation=45);
#plt.xlim([-10, 10]);

# ### Get day of the `week` and `hour`

# + language="spark"
# real_time = real_time.filter("arrival_delay is not NULL")
# real_time = real_time.withColumn("day_of_week", dayofweek(from_unixtime(col("arrival_time_schedule"))))
# real_time = real_time.withColumn("hour", hour(from_unixtime(col("arrival_time_schedule"))))
# -

# ### Get mean delay table

# + language="spark"
# real_time = real_time.select(["STOP_NAME", "produkt_id", "arrival_delay", "day_of_week", "hour"]).dropna()
# real_time = real_time.withColumn("arrival_delay", when(real_time["arrival_delay"] < 0, 0).when(col("arrival_delay").isNull(), 0)\
#                                  .otherwise(col("arrival_delay")/60)).cache()
# ## creating the table
# finalCols = ["STOP_NAME", "produkt_id", "day_of_week", "hour"]
#

# + language="spark"
# day = real_time.filter(real_time.day_of_week == 3).filter(real_time.hour == 6)
# day = day.groupBy(finalCols + ['arrival_delay']).count().cache()
#
#

# + language="spark"
# day.show()

# + language="spark"
# lambdas = day.groupBy(finalCols)\
#                 .agg(struct(collect_list("arrival_delay"), collect_list("count")).alias("delays"))\
#                 .withColumn("lambda", test_udf(col("delays"))).drop('delays').cache()

# + language="spark"
# lambdas.show()
#

# + language="spark"
# hour1 = lambdas

# + magic_args="-o hour1" language="spark"
# hour1
# -

hour1

# + language="spark"
# lambdas = None
# hour1.show()

# + language="spark"
# lambdas.filter(delays_distrib.STOP_NAME ==  "Zürich, Hardplatz").show()

# + language="spark"
# real_time = real_time.groupBy(finalCols).mean()\
#                      .select(finalCols + ["avg(arrival_delay)"])\
#                      .dropDuplicates()

# + language="spark"
# real_time = real_time.withColumnRenamed("avg(arrival_delay)", "inv_lambda")

# + language="spark"
# real_time.coalesce(1).write.format("com.databricks.spark.csv")\
#    .option("header", "true").save(REMOTE_PATH + "delay_distrib_final.csv")

# + language="spark"
# spark.read.orc("/data/sbb/part_orc/routes").show()
# -

# ## Create Route Name table

# + language="spark"
#
# ## creates route name !
# route_names = spark.read.orc("/data/sbb/part_orc/routes").withColumn("route_name", concat(col("route_desc"), lit(" "), col("route_short_name")))\
#                 .select(["route_id", "route_name", "route_desc"])\
#                 .dropDuplicates()
#
# ## get simple type
# complex_to_simple_type = {
#     "TGV":"Train",
#     "Eurocity":"Train",
#     "Regionalzug":"Train",
#     "RegioExpress":"Train",
#     "S-Bahn":"Train",
#     "Tram":"Tram",
#     "ICE":"Train",
#     "Bus":"Bus",
#     "Eurostar":"Train",
#     "Intercity":"Train",
#     "InterRegio":"Train",
#     "Extrazug":"Train"
# }
#
# get_simple_type = udf(lambda complex_type : complex_to_simple_type.get(complex_type,"unknown"))
#
# route_names = route_names.withColumn("transport_type", get_simple_type(col("route_desc")))\
#                 .drop("route_desc")
# route_names.show(3)

# + magic_args="-o route_names_and_types -n -1" language="spark"
# route_names_and_types = route_names
# #route_names.coalesce(1).write.format("com.databricks.spark.csv")\
# #   .option("header", "true").save(REMOTE_PATH + "route_names_and_types.csv")
# -

route_names_and_types

route_names_and_types.to_csv("../data/route_names_types.csv")


