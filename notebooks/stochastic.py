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

import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import expon
import numpy as np
from scipy.optimize import curve_fit

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
# stations = arrivals.select("STOP_NAME").dropDuplicates()
# real_time = real_time.join(stations, "STOP_NAME")
#
# # Compute the delay
# real_time = real_time.withColumn('arrival_time_schedule', 
#                                  unix_timestamp('arrival_time_schedule', "dd.MM.yyyy HH:mm"))\
#                 .withColumn('arrival_time_actual', unix_timestamp('arrival_time_actual', "dd.MM.yyyy HH:mm"))\
#                 .withColumn("arrival_delay", col("arrival_time_actual") - col("arrival_time_schedule"))\
#                 .filter("arrival_delay is not NULL")
#
# # Convert timestamps to day and hour
# real_time = real_time.withColumn("day_of_week", dayofweek(from_unixtime(col("arrival_time_schedule"))))\
#                     .withColumn("hour", hour(from_unixtime(col("arrival_time_schedule"))))
#                     
#
# # Clip negative delays to 0
# real_time = real_time.withColumn("arrival_delay", when(real_time["arrival_delay"] < 0, 0)\
#                                  .when(col("arrival_delay").isNull(), 0)\
#                                  .otherwise(col("arrival_delay")/60)).cache()
# -

# ## EDA of the delay distribution

# + language="spark"
# delays_distrib = real_time.filter("year == 2021").filter("month == 1")\
#                         .select(["STOP_NAME", "produkt_id", "arrival_delay"])\
#                         .groupBy(["STOP_NAME", "produkt_id","arrival_delay"]).count().cache()
#

# + magic_args="-o sample_dist" language="spark"
# sample_dist = delays_distrib.filter(delays_distrib.STOP_NAME ==  "Adliswil")\
#                             .filter(delays_distrib.produkt_id == "Zug")

# +
# Exponential distribution. We are going to fit parameter a
def pdf(x, a):
        return a * np.exp(-a * x)

def plot_delay_dist(sample_dist):
    fig = plt.figure(figsize=(20, 6))
    # Convert frequencies to density
    sample_dist = sample_dist.copy().sort_values("arrival_delay")
    sample_dist['count'] = sample_dist['count'] / sample_dist['count'].sum()
    
    
    g = sns.barplot(data=sample_dist.sort_values("arrival_delay"), x="arrival_delay", y="count")
    g.set_xticklabels(g.get_xticklabels(), rotation=45)
    
    # Fit the exponential
    popt, pcov = curve_fit(pdf, sample_dist.arrival_delay, sample_dist['count'])
    yy = pdf(sample_dist.arrival_delay, *popt)
    g.plot(range(len(sample_dist)), yy, '-o')
    
    station = sample_dist.STOP_NAME.iloc[0]
    transport_mean = sample_dist.produkt_id.iloc[0]
    g.set_xlabel("Delay (min)", fontsize=16)
    g.set_ylabel("Normalized count", fontsize=16)
    g.set_title(f"Delay distribution of trains at {station}", fontsize=16)
    g.text(15, 0.5, f'$\lambda=${popt[0]:.2f}', horizontalalignment='right', verticalalignment='top', fontsize=20)
    plt.show()
    
plot_delay_dist(sample_dist)
# -

# ### Fit distribution on for all (stops, transport type) pairs

# + language="spark"
# from pyspark.sql.functions import pandas_udf, PandasUDFType
# import numpy as np
# from scipy.optimize import curve_fit
#
# @udf
# def compute_lambda_udf(l):
#     counts = np.array(l[1])
#     popt, pcov = curve_fit(lambda x, a: a*np.exp(-a*x), l[0], counts / float(counts.sum()))
#     return float(popt[0])

# + tags=[] language="spark"
# # Show how it works on a subset
# delays_distrib.withColumn("arrival_delay", col("arrival_delay") /60)\
#                 .groupBy(['STOP_NAME','produkt_id'])\
#                 .agg(struct(collect_list("arrival_delay"), collect_list("count")).alias("delays"))\
#                 .withColumn("lambda", compute_lambda_udf(col("delays"))).show(4)

# + magic_args="-o lambdas " language="spark"
# # This cell takes ~30min
#
# finalCols = ["STOP_NAME", "produkt_id", "day_of_week", "hour"]
#
# # Since we only have the timetable of Wednesday, we only model delays on Wednesday
# # We also restrict the hours of the day from 5am to 10pm
# # Finally we count the frequencey of each delay to create the density function
# day = real_time.select(["STOP_NAME", "produkt_id", "arrival_delay", "day_of_week", "hour"]).dropna()\
#                 .filter(real_time.day_of_week == 3).filter((real_time.hour > 4) & (real_time.hour < 23))\
#                 .groupBy(finalCols + ['arrival_delay']).count()
#
# # From the density of delays we fit an exponential distribution and save the parameter lambda
# lambdas = day.groupBy(finalCols)\
#                 .agg(struct(collect_list("arrival_delay"), collect_list("count")).alias("delays"))\
#                 .withColumn("lambda", compute_lambda_udf(col("delays"))).drop('delays')

# + language="spark"
# lambdas.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite")\
#    .option("header", "true").save(REMOTE_PATH + "lambdas.csv")
