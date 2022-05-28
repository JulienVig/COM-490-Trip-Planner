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

import pandas as pd
import os

os.listdir("../data")

# ### Adding types and route names to routeStops

arrivals = pd.read_csv("../data/arrivalsRouteStops/arrivalsRouteStops.csv")
arrivals.head()


def load_csv(name, i=0, **kwargs):
    return pd.read_csv("../data/{name}/{file}".format(name=name, file=os.listdir("../data/{name}".format(name=name))[i]), **kwargs)


arrivals = load_csv("arrivalsRouteStops.csv")

routes_names_types = pd.read_csv("../data/route_names_types.csv").drop("Unnamed: 0", axis=1)

display(arrivals.head())
display(routes_names_types.head())

print("Number of arrivals : ", len(arrivals.index))

arrivals["route_id"] = arrivals.route_stop_id.apply(lambda x : x.split("$")[0])

arrivals_ext = arrivals.merge(routes_names_types, on="route_id")

print("Number of arrivals : ", len(arrivals_ext.index))

arrivals_ext.to_csv("../data/arrivals_ext.csv")


