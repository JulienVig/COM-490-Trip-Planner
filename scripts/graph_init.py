import sys
sys.path.insert(1,'../scripts')
from graph import Station, RouteStop, RouteStop, WalkingStop, Timetable
from denver import Denver

from os.path import join
import pandas as pd

DATA = '../data/'

def init_graph():
    
    """INIT STATION"""
    station_df = pd.read_csv(join(DATA, 'stations.csv'))
    if len(station_df) == 2:
        raise RuntimeError("Tables empty, run git lfs pull")
            
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
                       "InterRegio":"Train","Extrazug":"Train", 'Standseilbahn': "Train",
                       "Schnelles Nachtnetz":"Train", "Standseilbahn":"Train", "Luftseilbahn":"Train",                  
                      'Schiff': 'unknown'}

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
    hours = table_df.groupby("route_stop_id").apply(lambda row: list(sorted(row.arrival_time))).to_frame()
    
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
        
    def cleanup():
        for s in routestops.values():
            s.cleanup()
        for s in walking.values():
            s.cleanup()
        for s in stations.values():
            s.cleanup()


    return stations, timetable, cleanup