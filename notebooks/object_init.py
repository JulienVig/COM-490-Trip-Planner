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

import sys
sys.path.insert(1,'../scripts')
from graph import Station, RouteStopArr, RouteStopDep, WalkingStop, Timetable

from os.path import join
import pandas as pd

# %load_ext autoreload
# %autoreload 2

DATA = '../data/'

# # init stations

# +
# %time
station_df = pd.read_csv(join(DATA, 'stations_ext.csv'), index_col=0)

stations = {}
for i, row in station_df.iterrows():
    stations[row['STOP_NAME']]= Station(row['STOP_NAME'], row['STOP_NAME'], row['STOP_LAT'], row['STOP_LON'])
# -

station_df.head(1)

# # init routestops

routes = pd.read_csv(join(DATA,'route_names_types.csv'), index_col=0)
arrivals = pd.read_csv(join(DATA, 'arrivals_ttn.csv'), index_col=0).merge(routes, on='route_id')
departure = pd.read_csv(join(DATA, 'departures_ttn.csv'), index_col=0).merge(routes, on='route_id')

arrivals.head(1)

departure.head(1)

len(departure), len(arrivals)

# +
routestops_arr, routestops_dep = {}, {}                                      
   
for i, row in arrivals.iterrows():
    routestops_arr[row['end_route_stop_id']] = RouteStopArr(row['end_route_stop_id'],row['STOP_NAME'],
                                                            stations[row['STOP_NAME']], row['stop_sequence']*2, 
                                                            row['route_name'], row['transport_type'], 
                                                            row['travel_time'], None)
    
for i, row in departure.iterrows():
    routestops_dep[row['end_route_stop_id']] = RouteStopDep(row['end_route_stop_id'],row['STOP_NAME'],
                                                            stations[row['STOP_NAME']], -1, 
                                                            row['route_name'], row['transport_type'], 
                                                            row['travel_time'], None)

for i, row in arrivals.iterrows():
    routestops_arr[row['end_route_stop_id']].set_prev_stop(routestops_dep[row['target_end_route_stop_id']])


acc = 0
for i, row in departure.iterrows():
    current_id = row['end_route_stop_id']
    stop_seq = int(current_id[-3])
    if stop_seq != 1: # First route_stop_dep of the route doesn't have a prev stop
        previous_id = current_id[:-1] + 'A'
        if previous_id in routestops_arr:
            routestops_dep[current_id].set_prev_stop(routestops_arr[previous_id])
            routestops_dep[current_id].idx_on_route = stop_seq * 2 + 1
        else:
            acc+=1
print("Departure stops without previous arrival:", acc)
# -

# # init timetable

TARGET = 1.589388e+09

# +
# Dict[RouteStopDep, Tuple[List[int], List[Distrib]]]

table_df = pd.read_csv(join(DATA,  'timetable_ext.csv'))
table_df = table_df.drop_duplicates(['arrival_time_complete_unix', 'end_route_stop_id'])
# -

hours = table_df.groupby("end_route_stop_id").apply(lambda row: list(row.arrival_time_complete_unix)).to_frame()

timetable = {}
for i, row in hours.iterrows():
    timetable[row.name] = {'time': row[0]}

# + tags=[]
len(timetable)
# -

# ## delay distributions

import numpy as np

departure.transport_type.unique()

delays = pd.read_csv(join(DATA, 'lambdas_ext.csv'))
delays['produkt_id'] = delays.produkt_id.fillna('unknown')

delays.produkt_id.replace({'Zug': 'Train'})

for i, row in delays.iterrows():
    station_name = row['STOP_NAME']
    stations[station_name].delays[row['produkt_id']][row['hour'] - 1] = row['lambda']

# # init walking

# +
# %%time
walking_df = pd.read_csv(join(DATA, 'walking_stops_pairs.csv'), index_col=0)
walking = {}

def get_id(station_name):
    return station_name + '_walk'

for station_name in walking_df.STOP_NAME.unique():
    if station_name in stations.keys():
        walk_id = get_id(station_name)
        walking[walk_id] = WalkingStop(walk_id, station_name, stations[station_name], [])
        
for i, row in walking_df.iterrows():
    if row['STOP_NAME'] in stations.keys() and row['STOP_NAME_2'] in stations.keys():
        walk_id_1 = get_id(row['STOP_NAME'])
        walk_id_2 = get_id(row['STOP_NAME_2'])
        walking[walk_id_1].add_neighbor((walking[walk_id_2], row['walk_time']))
# -



# # Set station stops

# +
for stop in routestops_arr.values():
    stop.station.add_stop_arr(stop)
    
for stop in routestops_dep.values():
    stop.station.add_stop_dep(stop)
    
for stop in walking.values():
    stop.station.add_stop_dep(stop)
# -


