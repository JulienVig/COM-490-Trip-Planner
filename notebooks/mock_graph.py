import sys
sys.path.insert(1,'../scripts')
from graph import Station, RouteStop, WalkingStop, Timetable
from denver import Denver

from os.path import join
import pandas as pd
import math
import numpy as np
from collections import defaultdict
from datetime import datetime

DATA = '../data/'

# # Define graph parameters

# +
SIZE = 17 #km
MAX_LAT = SIZE / 110.574
MAX_LON = SIZE / (111.320 * math.cos(MAX_LAT))
N_STATIONS = 2000
N_ROUTE = 3000
MAX_ROUTE_LEN = 15
TRANSPORT_TYPES = ['Train', 'Bus', 'Tram', 'unknown']

square_ratio = 10 # number of max station per km
array_size = SIZE * square_ratio # stations can be as close as 250m
SQUARE_TO_METER = 1000 / float(square_ratio) # distance between each square of the grid
N_STATION_RANGE = 6 
TRAVEL_SPEED = 60/ 200 # 1min per 200 m
WALKING_SPEED = 60 / 50 # 60s / 50m
DAY_START_TS = 1589346000 # 13/05/2020 5am
DAY_END_TS = 1589407200 # 13/05/2020 10pm


# -

# # Create the graph instances

def find_stations_nearby(x, y, direction=None):
    # direction = 0 -> route goes North-East
    # 1 -> South-East
    # 2 -> North-West
    # 3 -> South-West
    if direction:
        range_i = np.arange(1, N_STATION_RANGE) * (-1 if direction % 2 else 1) * 2
        range_j = np.arange(1, N_STATION_RANGE) * (-1 if direction > 1 else 1) * 2
    else:
        range_i = range_j = list(range(-N_STATION_RANGE + 1, N_STATION_RANGE))
        range_i.remove(0)
    
    nearby_stations = []
    for i in range_i:
        for j in range_j:
            x_p, y_p = x + i, y + j
            #Check that we don't overflow or underflow
            if x_p > 0 and x_p < array_size and y_p > 0 and y_p < array_size:
                if grid[x_p, y_p]:
                    nearby_stations.append((x_p, y_p))
    return nearby_stations


def get_random_coord():
    return np.random.randint(0, array_size), np.random.randint(0, array_size)


# +
from math import sin, cos, sqrt, atan2, radians

def compute_walking_time(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c * 1000 # convert from km to m
    return int(round(distance *  WALKING_SPEED))


# +
np.random.seed(42)
grid = np.zeros((array_size, array_size)) # create a boolean grid for: is there an station at (x,y) ?

""" Create stations """
station_locations = {} # mapping between (x,y) and Station object
n_station_created = 0
while n_station_created < N_STATIONS: # create stations at random coord
    x, y = get_random_coord()
    if not grid[x,y]:
        grid[x,y] = 1
        station_name = f"station_{n_station_created}"
        lat = MAX_LAT * x / array_size
        lon = MAX_LON * y / array_size
        station_locations[(x,y)] = Station(station_name + '_id', station_name, lat, lon)
        n_station_created += 1

""" Create routes and routestops """
routes = defaultdict(list) # mapping between route_name to list of stops
route_stops = [] # mapping between routestop_name and routestop object
n_route_created = 0
while n_route_created < N_ROUTE: # Create routes starting at random station
    x,y = get_random_coord()
    while not grid[x,y]:
        x,y = get_random_coord()
    direction = np.random.randint(0, 4)
    route_name = f"route_{n_route_created}"
    transport_type = np.random.choice(TRANSPORT_TYPES)
    stations_nearby = find_stations_nearby(x, y, direction)
    
    n_stop = 0
    prev_stop = None
    while n_stop < MAX_ROUTE_LEN and len(stations_nearby) > 0:
        stop_name = f"{route_name}_stop_{n_stop}"
        if n_stop == 0:
            travel_time = 0
        else:
            # estimate distance with euclidean distance (in units of grid squares)
            # travel time is the travel speed times the distance converted to m
            travel_time = int(np.sqrt((prev_x - x)**2 + (prev_y - y)**2) * TRAVEL_SPEED * SQUARE_TO_METER)
        station = station_locations[x,y]
        curr_stop = RouteStop(stop_name, stop_name, station, n_stop, route_name, 
                              transport_type, travel_time, prev_stop, 'headsign')
        station.add_stop(curr_stop)
        routes[route_name].append((x, y, curr_stop))
        route_stops.append(curr_stop)
        prev_stop = curr_stop
        n_stop += 1
        prev_x, prev_y = x, y
        stations_nearby = find_stations_nearby(x, y, direction)
        if len(stations_nearby):
            x, y = stations_nearby[np.random.randint(0,high=len(stations_nearby))] # get next station
    if n_stop > 0:   
        n_route_created += 1
    
""" Create walkingstop """
walkstop_locations = {} # Mapping of coord to walkingstop object

# Init all the walkingstop objects
for (x,y), s in station_locations.items():
    stop_name = f"{s.station_name}_walk"
    walkstop = WalkingStop(stop_name, stop_name, s, )
    walkstop_locations[(x,y)] = walkstop

# Link if walking stops to its ids
for (x,y), w in walkstop_locations.items():
    for x_p, y_p in find_stations_nearby(x, y):
        neighbor = walkstop_locations[(x_p, y_p)]
        s1 = w.station
        s2 = neighbor.station
        walking_time = compute_walking_time(s1.latitude, s1.longitude, s2.latitude, s2.longitude)
        w.add_neighbor((neighbor, walking_time))
        
""" Create the time table """

table = {}
for routestop in route_stops:
    arrival_times = []
    random_freq = np.random.randint(5, 60) * 60 # between 5 and 60 min of frequency
    random_offset = np.random.randint(60) * 60 # the first trip is between DAY_START_TS and DAY_START_TS + random offset
    curr_ts = DAY_START_TS +  random_offset
    
    while curr_ts < DAY_END_TS:
        arrival_times.append(curr_ts)
        curr_ts += random_freq
    
    table[routestop] = arrival_times


# -

def cleanup():
    for r in route_stops:
        r.cleanup()
    for w in walkstop_locations.values():
        w.cleanup()
    for s in station_locations.values():
        s.cleanup()


# # Graph overview

count = 0
avg = 0
for r in routes.values():
    avg += len(r)
    if len(r) < MAX_ROUTE_LEN:
        count += 1
print(f"Over {len(routes)} routes, {count} have less than {MAX_ROUTE_LEN} stops")
print(f"On average, routes have {avg / len(routes):.2f} stops")

count = 0
for s in station_locations.values():
    count += len(s.stops)
print(f"Stations have in average {count / len(station_locations)} stops")

for x, y, stop in routes['route_0']:
    print(x, y, stop, stop.travel_time, stop.rw_prev_stop, stop.station, )

count = 0
travel_sum = 0
for w in walkstop_locations.values():
    count += len(w.neighbors)
    for n, time in w.neighbors:
        travel_sum += time
print(f"Walking stops have in average {count / len(walkstop_locations):.2f} neighbors")
print(f"There is in average {travel_sum / count:.2f}s between two neighboring walkingstops")

count = 0
for r, arr_times in table.items():
    count += len(arr_times)
print(f"There are a total of {count} arrival times")
print(f"In average, a route stop has {count/ len(table):.2f} arrival times")

# # Create nx graph for validation

# !pip install networkx

# +
import networkx as nx
G = nx.DiGraph()

for s in station_locations.values():
    G.add_node(s.station_name)
# -

for r in route_stops:
    if r.rw_prev_stop is not None:
        G.add_edge(r.station.station_name, r.rw_prev_stop.station.station_name)

# # Running Denver

target_arrival_time = datetime.now() # 5pm
threshold = 0
timetable = Timetable(table, threshold, target_arrival_time)

np.random.seed(48)
station_start, station_end = np.random.choice(list(station_locations.values()), size=2)
print(station_start, station_end)

# +
cleanup()

station_start, station_end = np.random.choice(list(station_locations.values()), size=2)
print("Start:", station_start, "End:", station_end)
print()

denver = Denver(threshold, station_end, station_start, timetable, False)
# %time sols = denver.run()
print()
if len(sols):
    print(sols[0])
else:
    print("No solutions found")
    print("NetworkX path exists:", nx.has_path(G, station_start.station_name, station_end.station_name))
# -


