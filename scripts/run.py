from graph import *
from denver import Denver

station_A = Station("A_node", "A", 0, 0)
station_B = Station("B_node", "B", 0, 0)
station_C = Station("C_node", "C", 0, 0)
station_G = Station("G_node", "G", 0, 0)
station_H = Station("H_node", "H", 0, 0)
station_I = Station("I_node", "I", 0, 0)
station_Y = Station("Y_node", "Y", 0, 0)

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

threshold = 0.7
table = Timetable(table_dict, threshold, target_arr_time)
multiple_sols = False
algo = Denver(threshold, station_Y, station_I, table, multiple_sols, target_arr_time)
sols = algo.denver()

print(f"Found {len(sols)} paths")
for i in range(len(sols)):
    print(sols[i])
