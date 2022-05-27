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


class Station(Node):
    def __init__(self, node_id, station_name: str, latitude: float, longitude: float, stops=None):
        super().__init__(node_id)
        self.station_name = station_name
        self.stops: List[Stop] = stops
        self.latitude = latitude
        self.longitude = longitude

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


# abstract
class Stop(Node):
    def __init__(self, node_id, stop_name, station):
        super().__init__(node_id)
        self.stop_name: str = stop_name
        self.station: Station = station


class RouteStop(Stop):
    def __init__(self, node_id, stop_name: str, station: Station, idx_on_route, route_name: str, transport_type: str):
        super().__init__(node_id, stop_name, station)
        self.idx_on_route: int = idx_on_route
        self.route_name: str = route_name
        self.transport_type = transport_type


class RouteStopArr(RouteStop):
    def __init__(self, node_id, stop_name, station, prev_stop, idx_on_route, route_name, travel_time, rw_prev_stop):
        super().__init__(node_id, stop_name, station, prev_stop, idx_on_route, route_name)
        self.travel_time: int = travel_time
        self.rw_prev_stop: RouteStopDep = rw_prev_stop


class RouteStopDep(RouteStop):
    def __init__(self, node_id, stop_name, station, prev_stop, idx_on_route, route_name, wait_time, rw_prev_stop):
        super().__init__(node_id, stop_name, station, prev_stop, idx_on_route, route_name)
        self.wait_time: int = wait_time
        self.rw_prev_stop: RouteStopArr = rw_prev_stop


class WalkingStop(Stop):
    def __init__(self, node_id, stop_name, station, neighbors=None):
        super().__init__(node_id, stop_name, station)
        self.neighbors: List[Tuple[WalkingStop, int]] = neighbors

    def set_neighbors(self, neighbors: List[Tuple['WalkingStop', int]]) -> None:
        self.neighbors = neighbors


class Marks:
    def __init__(self, blacklisted_route):
        self.blacklisted_route = blacklisted_route
        self.route_marks: Dict[str, RouteStop] = {}  # takes only earliest stop per route, RouteName is a string
        self.walk_marks: Set[WalkingStop] = set()
        self.station_marks: Set[Station] = set()

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
        self.table: Dict[RouteStopDep, Tuple[List[int], List[Distrib]]] = table  # timestamps are ascending in rw
        self.target_arr_time: int = target_arr_time
        self.INV_AVG_NB_OF_TRANSFER = 1 / 5

    def previous_arrival(self, stop: RouteStopDep, rw_station_arr_time: int) -> Tuple[int, int]:
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

    def assert_safe_transfer(self, stop: RouteStopDep, idx: int, wait_time: int,
                             threshold: float, acc_success: float) -> Tuple[float, bool]:
        """
        Assert using chosen heuristics if transfer is safe, and return success chance and safe boolean.
        The current heuristics are:
        1. The new success probability should still be above the input threshold
        2. The transfer success probability should be good enough to complete a journey with an avg nb of transfer.
            success_proba ^ avg_nb_transfer > threshold  =>  success_proba > pow(threshold, self.INV_AVG_NB_OF_TRANSFER)
        :return: (new_acc_success, is_safe)
        """
        stop_distrib = self.table[stop][1][idx]
        success_proba = stop_distrib.success_proba(wait_time)
        new_acc_success = acc_success * success_proba
        # We ensure that the risk taken at each transfer is sustainable enough for a trip with an avg nb of transfer
        is_safe = new_acc_success > threshold and success_proba > pow(threshold, self.INV_AVG_NB_OF_TRANSFER)
        return new_acc_success, is_safe

    def get_stop_arrival_time(self, stop: RouteStopDep, idx: int) -> int:
        return self.table[stop][0][idx]


class Path:
    def __init__(self, node_sequence, route_names, departure_time, walking_time, nb_transfers, string_representation):
        # List of nodes from real world departure station to arrival station
        self.node_sequence: List[Node] = node_sequence
        self.route_names = route_names
        self.departure_time = departure_time
        self.walking_time = walking_time
        self.nb_transfers = nb_transfers
        self.string_representation = string_representation

    @staticmethod
    def make(station: Station, stop: Stop, target_arr_time: int) -> 'Path':
        """Create a path from station and the given stop coming in"""
        route_names = set()
        node_sequence: List[Node] = [station, stop]
        node = stop.previous_node
        while node is not None:
            if isinstance(node, RouteStop):
                route_names.add(node.route_name)
            node_sequence.append(node)
            node = node.previous_node

        walking_time, nb_transfers, str_representation = Path._process_sequence(node_sequence, target_arr_time)
        departure_time = target_arr_time - node_sequence[0].arr_time
        return Path(node_sequence, route_names, departure_time, walking_time, nb_transfers, str_representation)

    @staticmethod
    def _process_sequence(node_sequence, target_arr_time):
        departure = node_sequence[0]
        strings = [f"Starting journey at {departure} at time "
                   f"{Path._convert_time_to_rw(departure.arr_time, target_arr_time)}\n"]

        nb_transfers = walking_time = 0
        current_route_start_time = 0
        current_trip_type = None  # either "transport" or "walk"
        prev_node = departure
        for n in node_sequence[1:]:
            if type(n) == Station:
                nb_transfers += 1

                rw_prev_arr_time = Path._convert_time_to_rw(prev_node.arr_time, target_arr_time)
                rw_arr_time = Path._convert_time_to_rw(n.arr_time, target_arr_time)
                if isinstance(n, Station):  # End of a trip
                    assert current_trip_type is not None
                    duration = rw_prev_arr_time - current_route_start_time
                    if current_trip_type == "transport":
                        strings.append(f"\tTo {n.station_name} at {Path._dt_to_str(rw_prev_arr_time)}\n")
                    elif current_trip_type == "walk":
                        strings.append(f"to {n.station_name} at {Path._dt_to_str(rw_prev_arr_time)}\n")
                        walking_time += duration
                    strings.append(f"\tDuration: {duration} seconds\n")

                if isinstance(n, RouteStop):  # Start of a new route
                    strings.append(f"Take line {n.route_name}:\n")
                    strings.append(f"\tFrom {n.station.station_name} at {Path._dt_to_str(rw_arr_time)}\n")
                    current_route_start_time = rw_arr_time
                    current_trip_type = "transport"

                if isinstance(n, WalkingStop):  # Start of a walk
                    strings.append(f"Walk from {n.station.station_name} at {Path._dt_to_str(rw_arr_time)}")
                    current_route_start_time = rw_arr_time
                    current_trip_type = "walk"

            prev_node = n

        rw_prev_arr_time = Path._convert_time_to_rw(prev_node.arr_time, target_arr_time)
        strings.append(f"Arrival at {node_sequence[-1]} at time {rw_prev_arr_time}\n")  # Last node is a station
        str_representation = "".join(strings)
        return walking_time, nb_transfers, str_representation

    @staticmethod
    def _convert_time_to_rw(time, target_arr_time) -> datetime:
        return datetime.fromtimestamp(target_arr_time - time)

    @staticmethod
    def _dt_to_str(time: datetime) -> str:
        return time.strftime('%Y-%m-%d %H:%M:%S')

    def __str__(self):
        return self.string_representation


class Distrib:
    def __init__(self, inv_lambda: float):
        dist = expon()
        self.success_proba = lambda x: dist.cdf(x, scale=inv_lambda)


class Solutions:
    def __init__(self, dep_time, target_station):
        self.sols: List[Path] = []
        self.best_target_arr_time: int = dep_time + 24*3600  # seconds in a day, since python has no max int value
        self.target_station: Station = target_station

    def sort_solutions(self) -> None:
        """Sort sols by Path', comparing departure time first, then walking time then number of changes"""
        # Negative departure_time because we want the latest departure time
        self.sols.sort(key=lambda path: (-path.departure_time, path.walking_time, path.nb_transfers))

    def n_solutions(self) -> int:
        return len(self.sols)

    def save(self, path: Path) -> None:
        self.sols.append(path)
