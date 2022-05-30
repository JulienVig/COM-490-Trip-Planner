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
from denver import Denver

from os.path import join
import pandas as pd

# %load_ext autoreload
# %autoreload 2

DATA = '../data/'


def init_graph(target):
    
    # STATION INIT
    station_df = pd.read_csv(join(DATA, 'stations_ext.csv'), index_col=0)
    stations = {}
    for i, row in station_df.iterrows():
        stations[row['STOP_NAME']]= Station(row['STOP_NAME'], row['STOP_NAME'], row['STOP_LAT'], row['STOP_LON'])
        
    # ROUTE STOPS INIT
    routes = pd.read_csv(join(DATA,'route_names_types.csv'), index_col=0)
    arrivals = pd.read_csv(join(DATA, 'arrivals_ttn.csv'), index_col=0).merge(routes, on='route_id')
    departure = pd.read_csv(join(DATA, 'departures_ttn.csv'), index_col=0).merge(routes, on='route_id')
    
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

    for i, row in departure.iterrows():
        current_id = row['end_route_stop_id']
        stop_seq = int(current_id[-3])
        if stop_seq != 1: # First route_stop_dep of the route doesn't have a prev stop
            previous_id = current_id[:-1] + 'A'
            if previous_id in routestops_arr:
                routestops_dep[current_id].set_prev_stop(routestops_arr[previous_id])
                routestops_dep[current_id].idx_on_route = stop_seq * 2 + 1
    
    # TIMETABLE INIT
    table_df = pd.read_csv(join(DATA,  'timetable_ext.csv'))
    table_df = table_df.drop_duplicates(['arrival_time_complete_unix', 'end_route_stop_id'])
    hours = table_df.groupby("end_route_stop_id").apply(lambda row: list(row.arrival_time_complete_unix)).to_frame()
    timetable = {}
    for i, row in hours.iterrows():
        if row.name in routestops_arr:
            timetable[routestops_arr[row.name]] = row[0]
            
    #  DELAYS
    delays = pd.read_csv(join(DATA, 'lambdas_ext.csv'))
    delays['produkt_id'] = delays.produkt_id.fillna('unknown')
    delays['produkt_id'] = delays.produkt_id.replace({'Zug': 'Train', 'Standseilbahn': 'unknown'})
    
    for i, row in delays.iterrows():
    station_name = row['STOP_NAME']
    if station_name in stations:
        stations[station_name].delays[row['produkt_id']][row['hour'] - 1] = row['lambda']
        
    # WALKING STOPS INIT
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
            
    # LINK STOP AND STATIONS
    for stop in routestops_arr.values():
    stop.station.add_stop_arr(stop)
    
    for stop in routestops_dep.values():
        stop.station.add_stop_dep(stop)

    for stop in walking.values():
        stop.station.add_stop_dep(stop)


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

routes

routes = pd.read_csv(join(DATA,'route_names_types.csv'), index_col=0)
arrivals = pd.read_csv(join(DATA, 'arrivals_ttn.csv'), index_col=0).merge(routes, on='route_id')
departure = pd.read_csv(join(DATA, 'departures_ttn.csv'), index_col=0).merge(routes, on='route_id')

arrivals.head(1).values

departure.head(1)

len(departure), len(arrivals)

# + tags=[]
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
table_df.head(1)
# -

hours = table_df.groupby("end_route_stop_id").apply(lambda row: list(row.arrival_time_complete_unix)).to_frame()
hours.head(1)

timetable = {}
for i, row in hours.iterrows():
    if row.name in routestops_arr:
        timetable[routestops_arr[row.name]] = row[0]

# + tags=[]
len(timetable)
# -

# ## delay distributions

import numpy as np

departure.transport_type.unique()

delays = pd.read_csv(join(DATA, 'lambdas_ext.csv'))
delays['produkt_id'] = delays.produkt_id.fillna('unknown')

delays.produkt_id.unique()

delays['produkt_id'] = delays.produkt_id.replace({'Zug': 'Train', 'Standseilbahn': 'unknown'})

for i, row in delays.iterrows():
    station_name = row['STOP_NAME']
    if station_name in stations:
        stations[station_name].delays[row['produkt_id']][row['hour'] - 1] = row['lambda']

# # init walking

# +
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
# # Running Denver

def cleanup():
    for s in routestops_arr.values():
        s.cleanup()
    for s in routestops_dep.values():
        s.cleanup()
    for s in walking.values():
        s.cleanup()
    for s in stations.values():
        s.cleanup()


cleanup()
threshold = 0.0
target_arr_time = 1589382000
g_start_2 = stations['Kloten Balsberg, Bahnhof']
g_start = stations['Zürich, Auzelg']
g_end = stations['Zürich, Leutschenbach']
actual_timetable = Timetable(timetable, target_arr_time)
multiple_sols = False
d = Denver(threshold, g_start_2, g_end, actual_timetable, multiple_sols, target_arr_time)
sols = d.denver()
print(sols)
for s in sols:
    print(s)


# for s in g_start.stops_dep:
#     print(s.route_name)
new_mickey = g_start.stops_dep[7]
print(new_mickey.stop_name)
print(new_mickey.rw_prev_stop.rw_prev_stop.stop_name)
print(stations['Kloten Balsberg, Bahnhof'])

# +
donald = stations['Zürich, Oerlikerhus']
print(len(donald.stops_arr))
    
mickey = donald.stops_arr[0]
print(mickey.rw_prev_stop.station)
print(mickey.route_name)

# +

stops1 = g_start_2.stops_arr[6]
stops2 = g_start_2.stops_arr[7]
stops3 = g_start_2.stops_arr[8]
while stops1 is not None:
    print(stops1.stop_name)
    stop1 = stops1.rw_prev_stop
print('finito lol')
while stops2 is not None:
    print(stops2.stop_name)
    stop2 = stops2.rw_prev_stop
print('finito lol')
while stops3 is not None:
    print(stops3.stop_name)
    stop3 = stops3.rw_prev_stop
print('finito lol')
# -


