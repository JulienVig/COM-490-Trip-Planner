# %load_ext autoreload
# %autoreload 2

import sys
sys.path.insert(1, '../scripts/')
from graph import Trip, RealSolution, Station, RouteStop, WalkingStop, Timetable
from frontend_utils import HTML_HEADER,HTML_TRIP, CSS_WIDGET, visualize_path, get_widgets
from denver import Denver
from datetime import datetime


def create_graph():
    station_A = Station("A_node", "A", 47.32112, 8.56321)
    station_B = Station("B_node", "B", 47.32612, 8.56621)
    station_C = Station("C_node", "C", 47.33312, 8.51421)
    station_G = Station("G_node", "G", 47.33812, 8.51221)
    station_H = Station("H_node", "H", 47.34712, 8.53521)
    station_I = Station("I_node", "I", 47.34412, 8.53221)
    station_Y = Station("Y_node", "Y", 47.33912, 8.54121)

    travel_times = 200

    stop_ACC = RouteStop("A-C_C_node", "A-C_C", station_C, 0, "C-A", 'Train', 0, None)
    stop_ACB = RouteStop("A-C_B_node", "A-C_B", station_B, 1, "C-A", 'Train', travel_times, stop_ACC)
    stop_ACA = RouteStop("A-C_A_node", "A-C_A", station_A, 2, "C-A", 'Train', travel_times, stop_ACB)
    stop_GHH = RouteStop("G-H_H_node", "G-H_H", station_H, 0, "H-G", 'Train', 0, None)
    stop_GHB = RouteStop("G-H_B_node", "G-H_B", station_B, 1, "H-G", 'Train', travel_times, stop_GHH)
    stop_GHG = RouteStop("G-H_G_node", "G-H_G", station_G, 2, "H-G", 'Train', travel_times, stop_GHB)
    stop_HII = RouteStop("H-I_I_node", "H-I_I", station_I, 0, "I-H", 'Train', 0, None)
    stop_HIH = RouteStop("H-I_H_node", "H-I_H", station_H, 1, "I-H", 'Train', travel_times, stop_HII)
    stop_BII = RouteStop("B-I_I_node", "B-I_I", station_I, 0, "I-B", 'Train', 0, None)
    stop_BIB = RouteStop("B-I_B_node", "B-I_B", station_B, 1, "I-B", 'Train', travel_times, stop_BII)

    walking_time = 50
    walking_Y = WalkingStop("Wa_Y_node", "Wa_Y", station_Y, None)
    walking_G = WalkingStop("Wa_G_node", "Wa_G", station_G, [(walking_Y, walking_time)])
    walking_Y.set_neighbors([(walking_G, walking_time)])

    # station_A.set_stops_dep([])
    station_A.set_stops([stop_ACA])
    station_B.set_stops([stop_ACB, stop_GHB, stop_BIB])
    station_C.set_stops([stop_ACC])
    station_G.set_stops([walking_G, stop_GHG])
    station_H.set_stops([stop_GHH, stop_HIH])
    station_I.set_stops([stop_HII, stop_BII])
    station_Y.set_stops([walking_Y])

    target_arr_time = 10000
    my_list = [i for i in range(0, target_arr_time, 30)]
    generic_table = my_list

    table_dict = {stop_ACB: generic_table, stop_ACA: generic_table, stop_GHB: generic_table,
                  stop_GHG: generic_table, stop_HIH: generic_table, stop_BIB: generic_table}

    all_stations = [station_A,station_B,station_C,station_G,station_H,station_I,station_Y]
    stations = {s.station_name: s for s in all_stations}
    return stations, table_dict


# +
#target_arr_time = 10000
#threshold = 0.7
#table = Timetable(table_dict, threshold, target_arr_time)
# -

stations, table_dict = create_graph()

all_widget_in_one, output = get_widgets(stations, table_dict)

from IPython.display import display
display(all_widget_in_one, output)






