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
from graph import Station, RouteStop, RouteStop, WalkingStop, Timetable
from denver import Denver

from os.path import join
import pandas as pd

# %load_ext autoreload
# %autoreload 2

DATA = '../data/'


def init_graph():
    
    """INIT STATION"""
    station_df = pd.read_csv(join(DATA, 'stations.csv'))
    stations = {}
    for i, row in station_df.iterrows():
        stations[row['STOP_NAME']]= Station(row['stop_id'], row['STOP_NAME'], row['STOP_LAT'], row['STOP_LON'])
        
    delays = pd.read_csv(join(DATA, 'lambdas.csv'))
    delays['produkt_id'] = delays.produkt_id.fillna('unknown')
    delays['produkt_id'] = delays.produkt_id.replace({'Zug': 'Train', 'Standseilbahn': 'unknown'})
    for i, row in delays.iterrows():
        station_name = row['STOP_NAME']
        if station_name in stations:
            stations[station_name].delays[row['produkt_id']][row['hour'] - 1] = row['lambda']
        
        
    """INIT ROUTESTOPS"""
    routestops_df = pd.read_csv(join(DATA,'routestops.csv'))
    routestops_df['route_name'] = routestops_df[["route_desc", "route_short_name"]].agg(' '.join, axis=1)

    transport_type = {"TGV":"Train","Eurocity":"Train","Regionalzug":"Train",
                                               "RegioExpress":"Train","S-Bahn":"Train","Tram":"Tram",
                                               "ICE":"Train","Bus":"Bus","Eurostar":"Train","Intercity":"Train",
                                               "InterRegio":"Train","Extrazug":"Train", 'Schiff': 'unknown'}

    routestops_df['route_desc'] = routestops_df.route_desc.replace(transport_type)
    routestops = {}                               
    for i, row in routestops_df.iterrows():
        routestops[row['route_stop_id']] = RouteStop(row['route_stop_id'],row['stop_name'],
                                                    stations[row['stop_name']], row['actual_stop_seq'], 
                                                    row['route_name'], row['route_desc'], 
                                                    row['travel_time'], None, row['trip_headsign'])

    for i, row in routestops_df.iterrows():
        if type(row['prev_route_stop_id']) != float:
            routestops[row['route_stop_id']].set_prev_stop(routestops[row['prev_route_stop_id']])
            
    table_df = pd.read_csv(join(DATA,  'timetable.csv'))
    hours = table_df.groupby("route_stop_id").apply(lambda row: list(row.arrival_time)).to_frame()
    
    timetable = {}
    for i, row in hours.iterrows():
        if row.name in routestops:
            timetable[routestops[row.name]] = row[0]
            
    """INIT WALKING"""
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
            
    """LINK STATIONS WITH ITS STOPS"""
    for stop in routestops.values():
        stop.station.add_stop(stop)

    for stop in walking.values():
        stop.station.add_stop(stop)
    return stations, routestops, walking, timetable

# # init stations

# %time
station_df = pd.read_csv(join(DATA, 'stations.csv'))

station_df.head(1)

len(station_df)

stations = {}
for i, row in station_df.iterrows():
    stations[row['STOP_NAME']]= Station(row['stop_id'], row['STOP_NAME'], row['STOP_LAT'], row['STOP_LON'])

# # init routestops

# +
routestops_df = pd.read_csv(join(DATA,'routestops.csv'))
routestops_df['route_name'] = routestops_df[["route_desc", "route_short_name"]].agg(' '.join, axis=1)

transport_type = {"TGV":"Train","Eurocity":"Train","Regionalzug":"Train",
                                           "RegioExpress":"Train","S-Bahn":"Train","Tram":"Tram",
                                           "ICE":"Train","Bus":"Bus","Eurostar":"Train","Intercity":"Train",
                                           "InterRegio":"Train","Extrazug":"Train", 'Schiff': 'unknown'}

routestops_df['route_desc'] = routestops_df.route_desc.replace(transport_type)

routestops_df.head(1)
# -

len(routestops_df)

# + tags=[]
routestops = {}                               
   
for i, row in routestops_df.iterrows():
    routestops[row['route_stop_id']] = RouteStop(row['route_stop_id'],row['stop_name'],
                                                stations[row['stop_name']], row['actual_stop_seq'], 
                                                row['route_name'], row['route_desc'], 
                                                row['travel_time'], None, row['trip_headsign'])

acc = 0
for i, row in routestops_df.iterrows():
    if type(row['prev_route_stop_id']) != float:
        routestops[row['route_stop_id']].set_prev_stop(routestops[row['prev_route_stop_id']])
    else:
        acc += 1
print(f"Number of routestops with no previous stops: {acc}")
# -

# # init timetable

# +
# Dict[RouteStopDep, Tuple[List[int], List[Distrib]]]

table_df = pd.read_csv(join(DATA,  'timetable.csv'))
table_df.head()
# -

hours = table_df.groupby("route_stop_id").apply(lambda row: list(row.arrival_time)).to_frame()
hours.head(1)

timetable = {}
for i, row in hours.iterrows():
    if row.name in routestops:
        timetable[routestops[row.name]] = row[0]

# + tags=[]
len(timetable)
# -

# ## delay distributions

import numpy as np

delays = pd.read_csv(join(DATA, 'lambdas.csv'))
delays['produkt_id'] = delays.produkt_id.fillna('unknown')

delays.produkt_id.unique()

delays['produkt_id'] = delays.produkt_id.replace({'Zug': 'Train', 'Standseilbahn': 'unknown'})

for i, row in delays.iterrows():
    station_name = row['STOP_NAME']
    if station_name in stations:
        stations[station_name].delays[row['produkt_id']][row['hour'] - 1] = row['lambda']

# # init walking

walking_df = pd.read_csv(join(DATA, 'walking_stops_pairs.csv'), index_col=0)
walking_df.head(1)
len(station_df.merge(walking_df.drop_duplicates('STOP_NAME')))

# +
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
for stop in routestops.values():
    stop.station.add_stop(stop)
    
for stop in walking.values():
    stop.station.add_stop(stop)


# -
# # Running Denver

def cleanup():
    for s in routestops.values():
        s.cleanup()
    for s in walking.values():
        s.cleanup()
    for s in stations.values():
        s.cleanup()


from datetime import datetime

stations, routestops, walking, timetable = init_graph()

cleanup()
threshold = 0.0
target_arr_time = datetime.now()
g_start_2 = stations['Kloten Balsberg, Bahnhof']
g_start = stations['Zürich, Auzelg']
g_end = stations['Zürich, Leutschenbach']
actual_timetable = Timetable(timetable, threshold, target_arr_time)
multiple_sols = False
denver = Denver(threshold, g_start_2, g_end, actual_timetable, multiple_sols)
sols = denver.run()
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


