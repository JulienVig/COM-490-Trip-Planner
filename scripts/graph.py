from typing import List, Dict, Set, Tuple
from bisect import bisect_right
from datetime import datetime
from scipy.stats import expon


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
    def __init__(self, node_id, station_name: str, latitude: float, longitude: float, stops_dep=[], stops_arr=[]):
        super().__init__(node_id)
        self.station_name = station_name
        self.stops_dep: List[Stop] = stops_dep
        self.stops_arr: List[Stop] = stops_arr
        self.latitude = latitude
        self.longitude = longitude

    def set_stops_dep(self, stops: List['Stop']) -> None:
        self.stops_dep = stops

    def set_stops_arr(self, stops: List['Stop']) -> None:
        self.stops_arr = stops

    def add_stop_dep(self, stop: 'Stop') -> None:
        self.stops_dep.append(stop)

    def add_stop_arr(self, stop: 'Stop') -> None:
        self.stops_arr.append(stop)

    def get_earliest_stop(self) -> 'Stop':
        earliest = self.stops_dep[0]
        for stop in self.stops_dep:
            if stop.arr_time < earliest.arr_time:
                earliest = stop
        return earliest

    def __str__(self):
        return self.station_name
    
    def __eq__(self, other: 'Station'):
        return self.station_name == self.station_name


# abstract
class Stop(Node):
    def __init__(self, node_id, stop_name, station):
        super().__init__(node_id)
        self.stop_name: str = stop_name
        self.station: Station = station


class RouteStop(Stop):
    def __init__(self, node_id, stop_name: str, station: Station, idx_on_route, route_name: str, transport_type: str,
                 travel_time: int):
        super().__init__(node_id, stop_name, station)
        self.idx_on_route: int = idx_on_route
        self.route_name: str = route_name
        self.transport_type: str = transport_type
        self.travel_time: int = travel_time


class RouteStopArr(RouteStop):
<<<<<<< HEAD
    def __init__(self, node_id, stop_name, station, idx_on_route, route_name, transport_type, travel_time, rw_prev_stop=None):
=======
    def __init__(self, node_id, stop_name, station, idx_on_route, route_name, transport_type, travel_time,
                 rw_prev_stop):
>>>>>>> hugo_tests
        super().__init__(node_id, stop_name, station, idx_on_route, route_name, transport_type, travel_time)
        self.rw_prev_stop: RouteStopDep = rw_prev_stop
    
    def set_prev_stop(self, rw_prev_stop):
        self.rw_prev_stop: RouteStopDep = rw_prev_stop

class RouteStopDep(RouteStop):
    def __init__(self, node_id, stop_name, station, idx_on_route, route_name, transport_type, wait_time, rw_prev_stop=None):
        super().__init__(node_id, stop_name, station, idx_on_route, route_name, transport_type, wait_time)
        self.rw_prev_stop: RouteStopArr = rw_prev_stop

    def set_prev_stop(self, rw_prev_stop):
        self.rw_prev_stop: RouteStopDep = rw_prev_stop

class WalkingStop(Stop):
    def __init__(self, node_id, stop_name, station, neighbors=None):
        super().__init__(node_id, stop_name, station)
        self.neighbors: List[Tuple[WalkingStop, int]] = neighbors

    def set_neighbors(self, neighbors: List[Tuple['WalkingStop', int]]) -> None:
        self.neighbors = neighbors
        
    def add_neighbor(self, neighbor: Tuple['WalkingStop', int])->None:
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
    def __init__(self, table, target_arr_time):
        # key is RouteStopDep.stop_name, value is (List[arrival_times], List[delay distributions])
        self.table: Dict[RouteStopArr, Tuple[List[int], List[Distrib]]] = table  # timestamps are ascending in rw
        self.target_arr_time: int = target_arr_time
        self.threshold = 0.0
        self.INV_AVG_NB_OF_TRANSFER = 1 / 5

    def previous_arrival(self, stop: RouteStopArr, rw_station_arr_time: int) -> Tuple[int, int]:
        """
        Real-world arguments, binary search in the stop's timetable, return arrival time and index in the table
        Arrival time is the largest arrival time that is smaller than rw_station_arr_time
        """
        stop_times = self.table[stop][0]
        prev_arr_time, idx = Timetable._find_previous_arr_time(stop_times, rw_station_arr_time)
        # if no previous arrival was found then prev_arr_time = -1 and idx = -1
        return prev_arr_time, idx

    @staticmethod
    def _find_previous_arr_time(a, x):
        # a is in increasing order, we want to find the timestamp at the left of x in a
        i = bisect_right(a, x)
        if i:  # if i != 0 then we have a valid previous arrival time
            return a[i - 1], i - 1
        return -1, -1  # returns -1 and 0 if no valid timestamp was found

    def assert_safe_transfer(self, stop: RouteStopArr, idx: int, wait_time: int,
                             threshold: float, acc_success: float) -> Tuple[float, bool]:
        """
        Assert using chosen heuristics if transfer is safe, and return success chance and safe boolean.
        The current heuristics are:
        1. The new success probability should still be above the input threshold
        2. The transfer success probability should be good enough to complete a journey with an avg nb of transfer.
            success_proba ^ avg_nb_transfer > threshold  =>  success_proba > pow(threshold, self.INV_AVG_NB_OF_TRANSFER)
        :return: (new_acc_success, is_safe)
        """
        wait_time = max(10, wait_time)
        stop_distrib = self.table[stop][1][idx]
        success_proba = stop_distrib.success_proba(wait_time)
        new_acc_success = acc_success * success_proba
        est_transfers_left = max(1.0, self.INV_AVG_NB_OF_TRANSFER - stop.n_changes)
        # We ensure that the risk taken at each transfer is sustainable enough for a trip with an avg nb of transfer
        is_safe = new_acc_success > threshold and success_proba > pow(threshold, est_transfers_left)
        return new_acc_success, is_safe

    def get_stop_arrival_time(self, stop: RouteStopArr, idx: int) -> int:
        return self.table[stop][0][idx]


class Distrib:
    def __init__(self, inv_lambda: float):
        dist = expon(scale=inv_lambda)
        self.success_proba = lambda x: dist.cdf(x/60)  # Distribution takes minutes as units


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
            if isinstance(n, RouteStop) and isinstance(prev_node, RouteStop) or type(n) == type(prev_node):
                curr_n_stops_crossed += 1

            # Case beginning of a transport trip
            elif isinstance(n, RouteStopDep):
                curr_station_dep = prev_node
                curr_route_name = n.route_name
                curr_dep_time = RealSolution._convert_time_to_rw(n.arr_time, target_arr_time)
                curr_trans_type = 'transport'
                curr_n_stops_crossed = 1

            # Case beginning of a walking trip
            elif isinstance(n, WalkingStop):
                curr_station_dep = prev_node
                curr_route_name = 'walk_route'
                curr_dep_time = RealSolution._convert_time_to_rw(n.arr_time, target_arr_time)
                curr_trans_type = 'walk'
                curr_n_stops_crossed = 1

            # Case end of a trip
            elif isinstance(n, Station):
                # Arrival time at the stop, not at the station
                arr_time = RealSolution._convert_time_to_rw(prev_node.arr_time, target_arr_time)
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

            else:
                raise TypeError(f"{type(prev_node)} followed by {type(n)}")
            
            prev_node = n

        success_probas.pop()
        confidence /= proba
        return RealSolution(trips, success_probas, confidence, walking_time)

    @staticmethod
    def _convert_time_to_rw(time, target_arr_time) -> datetime:
        return datetime.fromtimestamp(target_arr_time - time)


class Trip:
    def __init__(self, station_dep, station_arr, trans_type, duration, route_name, dep_time, n_stops_crossed):
        self.station_dep: Node = station_dep
        self.station_arr: Node = station_arr
        self.trans_type: str = trans_type
        self.duration: int = duration  # In seconds
        self.route_name: str = route_name
        self.dep_time: datetime = dep_time
        self.n_stops_crossed: int = n_stops_crossed
#         raise TypeError("We should import trip type from frontend, this is just temporary to avoid pycharm warnings")

    def __str__(self):
        raise TypeError("We should import trip type from frontend, this is just temporary to avoid pycharm warnings")

    def to_html(self):
        raise TypeError("We should import trip type from frontend, this is just temporary to avoid pycharm warnings")
