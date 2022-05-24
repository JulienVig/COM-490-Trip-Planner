from graph import *
from denver import Denver

station_A = Station("A_node", "A", 0, 0)
station_B = Station("B_node", "B", 0, 0)
station_C = Station("C_node", "C", 0, 0)
station_G = Station("G_node", "G", 0, 0)
station_H = Station("H_node", "H", 0, 0)
station_I = Station("I_node", "I", 0, 0)
station_Y = Station("Y_node", "Y", 0, 0)

travel_times = 50

stop_ACC = RouteStop("A-C_C_node", "A-C_C", station_C, None, travel_times, 0, "A-C")
stop_ACB = RouteStop("A-C_B_node", "A-C_B", station_B, stop_ACC, travel_times, 0, "A-C")
stop_ACA = RouteStop("A-C_A_node", "A-C_A", station_A, stop_ACB, travel_times, 0, "A-C")

stop_GHH = RouteStop("G-H_H_node", "G-H_H", station_H, None, travel_times, 0, "G-H")
stop_GHB = RouteStop("G-H_B_node", "G-H_B", station_B, stop_GHH, travel_times, 0, "G-H")
stop_GHG = RouteStop("G-H_G_node", "G-H_G", station_G, stop_GHB, travel_times, 0, "G-H")

stop_HII = RouteStop("H-I_I_node", "H-I_I", station_I, None, travel_times, 0, "H-I")
stop_HIH = RouteStop("H-I_H_node", "H-I_H", station_H, stop_HII, travel_times, 0, "H-I")


walking_time = 50
walking_Y = WalkingStop("Wa_Y_node", "Wa_Y", station_Y, None)
walking_G = WalkingStop("Wa_G_node", "Wa_G", station_G, [(walking_Y, walking_time)])
walking_Y.set_neighbors([(walking_G, walking_time)])

station_A.set_stops([stop_ACA])
station_B.set_stops([stop_ACB, stop_GHB])
station_C.set_stops([stop_ACC])
station_G.set_stops([stop_GHG, walking_G])
station_H.set_stops([stop_GHH, stop_HIH])
station_I.set_stops([stop_HII])
station_Y.set_stops([walking_Y])

my_list = [i for i in range(0, 1200, 30)]
generic_table = (my_list, [Distrib() for i in range(len(my_list))])

table_dict = {stop_ACC: generic_table, stop_ACB: generic_table, stop_ACA: generic_table, stop_GHH: generic_table,
              stop_GHB: generic_table, stop_GHG: generic_table, stop_HII: generic_table, stop_HIH: generic_table}

table = Timetable(table_dict, 1000)
algo = Denver(0, 0, station_Y, station_I, table, 1, 1000)
print(algo.denver()[0])