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
wait_times = 10

stop_ACC_dep = RouteStopDep("A-C_C_node_dep", "A-C_C", station_C, 0, "A-C", 'Zug', 0, None)
stop_ACB_arr = RouteStopArr("A-C_B_node_arr", "A-C_B", station_B, 1, "A-C", 'Zug', travel_times, stop_ACC_dep)
stop_ACB_dep = RouteStopDep("A-C_B_node_dep", "A-C_B", station_B, 1, "A-C", 'Zug', wait_times, stop_ACB_arr)
stop_ACA_arr = RouteStopArr("A-C_A_node_arr", "A-C_A", station_A, 2, "A-C", 'Zug', travel_times, stop_ACB_dep)

stop_GHH_dep = RouteStopDep("G-H_H_node_dep", "G-H_H", station_H, 0, "G-H", 'Zug', 0, None)
stop_GHB_arr = RouteStopArr("G-H_B_node_arr", "G-H_B", station_B, 1, "G-H", 'Zug', travel_times, stop_GHH_dep)
stop_GHB_dep = RouteStopDep("G-H_B_node_dep", "G-H_B", station_B, 1, "G-H", 'Zug', wait_times, stop_GHB_arr)
stop_GHG_arr = RouteStopArr("G-H_G_node_arr", "G-H_G", station_G, 2, "G-H", 'Zug', travel_times, stop_GHB_dep)

stop_HII_dep = RouteStopDep("H-I_I_node_dep", "H-I_I", station_I, 0, "H-I", 'Zug', 0, None)
stop_HIH_arr = RouteStopArr("H-I_H_node_arr", "H-I_H", station_H, 1, "H-I", 'Zug', travel_times, stop_HII_dep)


walking_time = 50
walking_Y = WalkingStop("Wa_Y_node", "Wa_Y", station_Y, None)
walking_G = WalkingStop("Wa_G_node", "Wa_G", station_G, [(walking_Y, walking_time)])
walking_Y.set_neighbors([(walking_G, walking_time)])

# station_A.set_stops_dep([])
station_A.set_stops_arr([stop_ACA_arr])
station_B.set_stops_dep([stop_ACB_dep, stop_GHB_dep])
station_B.set_stops_arr([stop_ACB_arr, stop_GHB_arr])
station_C.set_stops_dep([stop_ACC_dep])
# station_C.set_stops_arr([])
station_G.set_stops_dep([walking_G])
station_G.set_stops_arr([stop_GHG_arr, walking_G])
station_H.set_stops_dep([stop_GHH_dep])
station_H.set_stops_arr([stop_HIH_arr])
station_I.set_stops_dep([stop_HII_dep])
# station_I.set_stops_arr([])
station_Y.set_stops_dep([walking_Y])
station_Y.set_stops_arr([walking_Y])

target_arr_time = 10000
my_list = [i for i in range(0, target_arr_time, 30)]
generic_table = (my_list, [Distrib(1) for i in range(len(my_list))])

table_dict = {stop_ACB_arr: generic_table, stop_ACA_arr: generic_table, stop_GHB_arr: generic_table, 
              stop_GHG_arr: generic_table, stop_HIH_arr: generic_table}

table = Timetable(table_dict, target_arr_time)
threshold = 0
n_sols_expected = 1
algo = Denver(threshold, station_Y, station_I, table, n_sols_expected, target_arr_time)
print(algo.denver()[0])