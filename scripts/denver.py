from graph import Station, Timetable, RealSolution, Marks, RouteStop, WalkingStop, Node
from typing import List
from datetime import datetime

TRANSFER_TIME = 120  # 2 minutes
FIRST_STATION_TS = 0  # Chosen timestamp of the first station in graph (and last in real world)


class Denver:
    def __init__(self, threshold: float, g_start: Station, g_end: Station,
                 timetable: Timetable, multiple_sols: bool):
        self.g_start = g_start
        self.g_end = g_end
        self.timetable = timetable
        self.multiple_sols = multiple_sols

    # prefix g_ is for reversed graph, prefix i_ is for input values
    def run(self, blacklisted_route: str = "") -> List[RealSolution]:
        marks = Marks(blacklisted_route, FIRST_STATION_TS)
        self.init_first_station(self.g_start, marks, self.timetable)
        while not marks.empty():
            self.update_lines(marks)
            self.update_walks(marks)
            self.update_stations(marks, self.timetable)

        if self.g_end.previous_node is None:
            return []

        nodes = self.make_best_path(self.g_end)
        sol = RealSolution.generate(nodes, self.timetable.target_arr_time)
        sols = [sol]
        if self.multiple_sols:
            self.multiple_sols = False
            for route_removed in sol.route_names:
                # Run the same algorithms on a graph with one route from the solution removed
                # TODO : Reset graph
                sols += self.denver(route_removed)

        sols.sort(key=lambda s: (s.dep_time, -s.walking_time, -s.n_transfers), reverse=True)
        return sols

    @staticmethod
    def init_first_station(g_start: Station, marks: Marks, timetable: Timetable) -> None:
        g_start.update_arrival(FIRST_STATION_TS, None, 1, 0)
        for stop in g_start.stops:
            if isinstance(stop, RouteStop):
                dep_time, _ = timetable.previous_arrival(stop, FIRST_STATION_TS)
                stop.update_arrival(dep_time, g_start, 1, 0)
                marks.mark_route(stop)
            elif isinstance(stop, WalkingStop):
                stop.update_arrival(FIRST_STATION_TS, g_start, 1, 0)
                marks.mark_walk(stop)

    @staticmethod
    def update_lines(marks: Marks) -> None:
        # The stop associated with each route is the first stop with a new arr_time on this route
        for route_name, stop in marks.route_marks.items():
            while stop.rw_prev_stop is not None:
                new_arr_time = stop.arr_time + stop.travel_time
                # Local & target pruning
                if new_arr_time < marks.best_target_arr_time and new_arr_time < stop.rw_prev_stop.arr_time:
                    # Update the next stop
                    stop.rw_prev_stop.update_arrival(new_arr_time, stop, stop.acc_success, stop.n_changes)
                    # Mark next stop's station. Will be done twice but not a problem wrt correctness
                    marks.mark_station(stop.rw_prev_stop.station)
                stop = stop.rw_prev_stop
        marks.flush_routes()  # Unmark all routes

    @staticmethod
    def update_walks(marks: Marks) -> None:
        """Update WalkingStop's neighbors until it converges"""
        while not marks.walk_empty():
            stop = marks.pop_walk()
            for neighbor, walk_time in stop.neighbors:
                new_arr_time = stop.arr_time + walk_time
                if neighbor.arr_time > new_arr_time:
                    neighbor.update_arrival(new_arr_time, stop, stop.acc_success, stop.n_changes)
                    marks.mark_walk(neighbor)
                    marks.mark_station(neighbor.station)

    def update_stations(self, marks: Marks, timetable: Timetable) -> None:
        for station in marks.station_marks:
            earliest_stop = station.get_earliest_stop()  # Get the stop with the earliest arrival time
            new_arr_time = earliest_stop.arr_time + TRANSFER_TIME

            # We update the station and station's stops if:
            # - the new earliest arrival time is different from the current one
            #   (if it's different then it's always better than the previous one, o.w. it wouldn't have changed)
            # - the previous node is not the same as new earliest stop
            # - the accumulated journey success probability is different
            if station.arr_time != new_arr_time or station.previous_node != earliest_stop \
                    or station.acc_success != earliest_stop.acc_success:
                station.update_arrival(new_arr_time, earliest_stop, earliest_stop.acc_success, earliest_stop.n_changes)
                # For each of the station's stops, see if the new earliest trip improves its arr_time
                rw_station_arr_time = timetable.target_arr_time - station.arr_time + FIRST_STATION_TS
                for stop in station.stops:
                    if isinstance(stop, RouteStop):
                        # Note : All calls to timetable functions must take real-world arguments. Stored returned values
                        # are named relative to the graph and not the real world.
                        rw_previous_arrival_time, idx = timetable.previous_arrival(stop, rw_station_arr_time)
                        if idx == -1:  # No previous departure was found
                            continue
                        wait_time = rw_station_arr_time - rw_previous_arrival_time
                        new_acc_success, safe = timetable.assert_safe_transfer(stop, wait_time,
                                                                               rw_previous_arrival_time,
                                                                               timetable.threshold, stop.acc_success)
                        # Check previous arrival times until we find that is safe enough or checked all of them
                        idx -= 1
                        while not safe and station.arr_time + wait_time < stop.arr_time and idx >= 0:
                            rw_previous_arrival_time = timetable.get_stop_arrival_time(stop, idx)
                            wait_time = rw_station_arr_time - rw_previous_arrival_time
                            new_acc_success, safe = timetable.assert_safe_transfer(stop, wait_time,
                                                                                   rw_previous_arrival_time,
                                                                                   timetable.threshold,
                                                                                   stop.acc_success)
                            idx -= 1

                        if safe and station.arr_time + wait_time < stop.arr_time:
                            stop.update_arrival(station.arr_time + wait_time, station, new_acc_success,
                                                station.n_changes + 1)
                            marks.mark_route(stop)
                    elif isinstance(stop, WalkingStop):
                        if station.arr_time < stop.arr_time:
                            stop.update_arrival(station.arr_time, station, station.acc_success, station.n_changes + 1)
                            marks.mark_walk(stop)
                    else:
                        raise TypeError("Station is referencing a Stop that is not RouteStopArr or WalkingStop")
                if station == self.g_end:
                    marks.best_target_arr_time = station.arr_time
        marks.flush_stations()

    @staticmethod
    def make_best_path(station: Station) -> List[Node]:
        """Create a path from station and the given stop coming in"""
        node_sequence: List[Node] = []
        n = station
        while n is not None:
            node_sequence.append(n)
            n = n.previous_node

        return node_sequence
