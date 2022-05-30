from typing import List, Dict, Set, Tuple
from bisect import bisect_right
from datetime import datetime
from trip import Trip
import numpy as np


class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.arr_time: int = 2551596325  # Earliest arrival time, init s. t. it's always higher than reachable in 1 day
        self.previous_node: Node = None  # Previous node on shortest path
        self.acc_success: float = 1  # Current accumulated success
        self.n_changes = 0  # Number of changes currently done during the whole trip

    def update_arrival(self, new_arr_time: int, new_prev_node: 'Node', new_acc_success: float, new_n_changes) -> None:
        self.arr_time = new_arr_time
        self.previous_node = new_prev_node
        self.acc_success = new_acc_success
        self.n_changes = new_n_changes

    def __eq__(self, other: 'Node'):
        return self.node_id == other.node_id

    def __hash__(self):
        return hash(self.node_id)

    def cleanup(self):
        self.arr_time: int = 2551596325
        self.previous_node: Node = None
        self.acc_success: float = 1
        self.n_changes = 0


class Station(Node):
    def __init__(self, node_id, station_name: str, latitude: float, longitude: float):
        super().__init__(node_id)
        self.station_name = station_name
        self.stops: List[Stop] = stops if stops is not None else []
        self.latitude = latitude
        self.longitude = longitude
        self.delays: Dict[str, np.array] = {'Bus': np.ones(24)*100, 'Tram': np.ones(24)*100,
                                            'Train': np.ones(24)*100, 'unknown': np.ones(24)*100}

    def set_stops(self, stops: List['Stop']) -> None:
        self.stops = stops

    def add_stop(self, stop: 'Stop') -> None:
        self.stops.append(stop)

    def get_earliest_stop(self) -> 'Stop':
        earliest = self.stops[0]
        for stop in self.stops:
            if stop.arr_time < earliest.arr_time:
                earliest = stop
        return earliest

    def __str__(self):
        return self.station_name

    def __eq__(self, other: 'Station'):
        return self.station_name == other.station_name

    def __hash__(self):
        return hash(self.station_name)


# abstract
class Stop(Node):
    def __init__(self, node_id, stop_name, station):
        super().__init__(node_id)
        self.stop_name: str = stop_name
        self.station: Station = station

    def __str__(self):
        return self.stop_name

class RouteStop(Stop):
    def __init__(self, node_id, stop_name: str, station: Station, idx_on_route, route_name: str, transport_type: str,
                 travel_time: int, rw_prev_stop: 'RouteStop'):
        super().__init__(node_id, stop_name, station)
        self.idx_on_route: int = idx_on_route
        self.route_name: str = route_name
        self.transport_type: str = transport_type
        self.travel_time: int = travel_time
        self.rw_prev_stop: 'RouteStop' = rw_prev_stop


class WalkingStop(Stop):
    def __init__(self, node_id, stop_name, station, neighbors=None):
        super().__init__(node_id, stop_name, station)
        self.neighbors: List[Tuple[WalkingStop, int]] = neighbors if neighbors is not None else []

    def set_neighbors(self, neighbors: List[Tuple['WalkingStop', int]]) -> None:
        self.neighbors = neighbors

    def add_neighbor(self, neighbor: Tuple['WalkingStop', int]) -> None:
        self.neighbors.append(neighbor)


class Marks:
    def __init__(self, blacklisted_route, dep_time):
        self.blacklisted_route = blacklisted_route
        self.route_marks: Dict[str, RouteStop] = {}  # takes only earliest stop per route, RouteName is a string
        self.walk_marks: Set[WalkingStop] = set()
        self.station_marks: Set[Station] = set()
        self.best_target_arr_time = dep_time + 24 * 3600

    def mark_station(self, station: Station) -> None:
        self.station_marks.add(station)

    def mark_route(self, route_stop: RouteStop) -> None:
        if route_stop.route_name != self.blacklisted_route:
            current = self.route_marks.get(route_stop.route_name, None)
            if current is None or current.idx_on_route > route_stop.idx_on_route:
                self.route_marks[route_stop.route_name] = route_stop

    def mark_walk(self, walking_stop: WalkingStop) -> None:
        self.walk_marks.add(walking_stop)

    def flush_routes(self) -> None:
        self.route_marks = {}

    def empty(self) -> bool:
        return not (self.route_marks or self.walk_marks or self.station_marks)

    def walk_empty(self) -> bool:
        return len(self.walk_marks) == 0

    def pop_walk(self) -> WalkingStop:
        return self.walk_marks.pop()

    def flush_stations(self):
        self.station_marks = set()


class Timetable:
    def __init__(self, table, threshold, target_arr_time):
        # key is RouteStopDep.stop_name, value is (List[arrival_times], List[delay distributions])
        self.table: Dict[RouteStop, List[int]] = table  # timestamps are ascending in rw
        self.target_arr_time: int = target_arr_time
        self.threshold: int = threshold
        self.AVG_NB_OF_TRANSFER = 5

    def set_target_time(self, target_arr_time):
        self.target_arr_time = target_arr_time

    def previous_arrival(self, stop: RouteStop, rw_station_arr_time: int) -> Tuple[int, int]:
        """
        Real-world arguments, binary search in the stop's timetable, return arrival time and index in the table
        Arrival time is the largest arrival time that is smaller than rw_station_arr_time
        """
        stop_times = self.table.get(stop, None)
        if stop_times is not None: 
            prev_arr_time, idx = Timetable._find_previous_arr_time(stop_times, rw_station_arr_time)
            # if no previous arrival was found then prev_arr_time = -1 and idx = -1
            return prev_arr_time, idx
        return -1, -1

    @staticmethod
    def _find_previous_arr_time(a, x):
        # a is in increasing order, we want to find the timestamp at the left of x in a
        i = bisect_right(a, x)
        if i:  # if i != 0 then we have a valid previous arrival time
            return a[i - 1], i - 1
        return -1, -1  # returns -1 and 0 if no valid timestamp was found

    def assert_safe_transfer(self, stop: RouteStop, wait_time: int, rw_previous_arrival_time: int,
                             threshold: float, acc_success: float) -> Tuple[float, bool]:
        """
        Assert using chosen heuristics if transfer is safe, and return success chance and safe boolean.
        The current heuristics are:
        1. The new success probability should still be above the input threshold
        2. The transfer success probability should be good enough to complete a journey with an avg nb of transfer.
            success_proba ^ avg_nb_transfer > threshold  =>  success_proba > pow(threshold, 1 / avg_nb_transfer)
        :return: (new_acc_success, is_safe)
        """
        wait_time = max(10, wait_time)
        current_hour: int = RealSolution.convert_time_to_rw(rw_previous_arrival_time, self.target_arr_time).hour
        lambda_ = stop.station.delays[stop.transport_type][current_hour]
        success_proba = 1 - np.exp(- lambda_ * wait_time / 60)
        new_acc_success = acc_success * success_proba
        est_transfers_left = max(1.0, self.AVG_NB_OF_TRANSFER - stop.n_changes)
        # We ensure that the risk taken at each transfer is sustainable enough for a trip with an avg nb of transfer
        is_safe = new_acc_success > threshold and success_proba > pow(threshold, 1 / est_transfers_left)
        return new_acc_success, is_safe

    def get_stop_arrival_time(self, stop: RouteStop, idx: int) -> int:
        return self.table[stop][0][idx]


class RealSolution:
    def __init__(self, trips, success_probas, confidence, walking_time):
        self.trips: List[Trip] = trips
        self.success_probas: List[float] = success_probas
        self.confidence: float = confidence
        self.dep_time: datetime = trips[0].dep_time
        self.walking_time: int = walking_time
        self.n_transfers: int = len(success_probas)
        self.route_names: List[str] = list(map(lambda trip: trip.route_name, trips))

    @staticmethod
    def generate(nodes: List[Node], target_arr_time) -> 'RealSolution':
        trips: List[Trip] = []
        success_probas: List[float] = []
        confidence = 1.0

        curr_station_dep: Node = None
        curr_route_name: str = None
        curr_dep_time: datetime = None
        curr_trans_type: str = None
        curr_n_stops_crossed: int = None

        walking_time = 0
        proba = 0.0
        prev_node = nodes[0]
        for n in nodes[1:]:
            # Case 2 RouteStop, or 2 WalkingStop : we are on some route
            if type(n) == type(prev_node):
                curr_n_stops_crossed += 1

            # Case beginning of a transport trip
            elif isinstance(n, RouteStop):
                curr_station_dep = prev_node
                curr_route_name = n.route_name
                curr_dep_time = RealSolution.convert_time_to_rw(n.arr_time, target_arr_time)
                curr_trans_type = n.transport_type
                curr_n_stops_crossed = 1

            # Case beginning of a walking trip
            elif isinstance(n, WalkingStop):
                curr_station_dep = prev_node
                curr_route_name = ''
                curr_dep_time = RealSolution.convert_time_to_rw(n.arr_time, target_arr_time)
                curr_trans_type = 'Walk'
                curr_n_stops_crossed = 1

            # Case end of a trip
            elif isinstance(n, Station):
                # Arrival time at the stop, not at the station
                arr_time = RealSolution.convert_time_to_rw(prev_node.arr_time, target_arr_time)
                curr_station_arr = n
                curr_duration = (arr_time - curr_dep_time).total_seconds()
                if isinstance(prev_node, WalkingStop):
                    walking_time += curr_duration
                trip_done = Trip(curr_station_dep, curr_station_arr, curr_trans_type, curr_duration, curr_route_name,
                                 curr_dep_time, curr_n_stops_crossed)
                trips.append(trip_done)
                proba = prev_node.acc_success / n.acc_success
                success_probas.append(proba)
                confidence *= proba

            prev_node = n

        success_probas.pop()
        confidence /= proba
        return RealSolution(trips, success_probas, confidence, walking_time)

    def __str__(self):
        strings = []
        for t in self.trips:
            strings.append(f"{t.station_dep} to {t.station_arr}\n")

        return ''.join(strings)

    @staticmethod
    def convert_time_to_rw(time, target_arr_time) -> datetime:
        return datetime.fromtimestamp(target_arr_time - time)
