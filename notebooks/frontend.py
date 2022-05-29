import time
def pretify_seconds(seconds):
    return time.strftime('%H:%M:%S', time.gmtime(seconds))


# +
def process_sequence(node_sequence):
    departure = node_sequence[0]

    # list of Trip(station0, station1, transport_type, time)
    processed = []

    walking_time = 0
    current_route_start_time = 0
    current_trip_type = None 
    prev_node = departure
    curr_dep_station = None
    curr_route_name = None
    
    for n in node_sequence:
        if (not type(n) == type(prev_node)) and not (isinstance(n, RouteStop) and isinstance(prev_node, RouteStop)):
            if isinstance(n, Station) and current_trip_type is not None:  # End of a trip
                duration = prev_node.arr_time - current_route_start_time
                if current_trip_type == "transport":
                    current_trip_type = "Bus"
                    
                elif current_trip_type == "walk":
                    walking_time += duration
                    current_trip_type = "Walking"
                processed.append(Trip(curr_dep_station, n, current_trip_type, duration, curr_route_name, current_route_start_time))
                curr_route_name = None
                
            if isinstance(n, RouteStop):  # Start of a new route
                curr_route_name = n.route_name
                current_route_start_time = n.arr_time
                current_trip_type = "transport"
                curr_dep_station = n.station

            if isinstance(n, WalkingStop):  # Start of a walk
                current_route_start_time = n.arr_time
                current_trip_type = "walk"
                curr_dep_station = n.station

        prev_node = n
        
    return processed, pretify_seconds(walking_time), len(processed)-1

def process_path(path):
    return process_sequence(path.node_sequence)


# -

class RealSolution:
    def __init__(self, trips, success_probas, confidence, walking_time):   
        self.trips: List[Trip] = trips
        self.success_probas: List[float] = success_probas
        self.confidence: float = confidence
        self.walking_time: int = walking_time
    
    @staticmethod
    def generate(p: "Path") -> "RealSolution":
        return RealSolution([], [], 0.0)


# +
import sys
sys.path.insert(1, '../scripts/')
from trip import Trip

# class Trip:
#     def __init__(self, station_dep, station_arr, trans_type, duration, route_name, dep_time, n_stops_crossed):
#         self.station_dep: Node = station_dep
#         self.station_arr: Node = station_arr
#         self.trans_type: String = trans_type
#         self.duration: int = duration
#         self.route_name: String = route_name
#         self.dep_time: datetime = dep_time
#         self.n_stops_crossed: int = n_stops_crossed
            
#     def __str__(self):
#         if (self.trans_type=="Walking"):
#             prefix = "Walk"
#         else:
#             prefix = f"Take the line {self.route_name}"
#         return prefix+f" from {self.station_dep.station_name} to {self.station_arr.station_name} during {pretify_seconds(self.duration)}. Departure : {self.dep_time} Arrival : {self.dep_time+timedelta(seconds=self.duration)}"
    
#     def to_html(self):
#         if (self.trans_type=="Walking"):
#             head = "Walk"
#         else:
#             head=self.route_name
        
#         html_str = HTML_TRIP.format(self.trans_type, self.route_name, self.station_dep.station_name, self.station_arr.station_name, self.dep_time, self.dep_time+timedelta(seconds=self.duration), self.n_stops_crossed, self.duration)
#         return html_str

# +
# format with station dep name, station arr name, total time (nice string), walk time (nice string), success proba, transfers
HTML_HEADER = """
<li class="myheader">
    <p>Your trip from <b>{}</b> to <b>{}</b></p>
    <p>Total time : <b>{}</b> Walking time : <b>{}</b></p>
    <p>Success probability : <b>{}</b></p>
    <p>Transfers : <b>{}</b></p>
</li>
"""
# format with trans type, line number, dep station name, arr station name, dep time, arr time, #stops, duration (nice string)
HTML_TRIP = """
<li class="myli">
  <h3 class="myh3">{} {}</h3>
  <div class="stops">
    <p class="stop">{}</p>
    <p class="stop">=================></p>
    <p class="stop">{}</p>
  </div>
  <div class="stops">
    <p class="stop">{}</p>
    <p class="stop">{}</p>
  </div>
  <p>Trip duration : {} stops ({}) </p>
</li>

"""


CSS_WIDGET = """
<style>
.myul {
  list-style-type: none;
  width: 500px;
}
.myh3 {
  font: bold 20px/1.7 Helvetica, Verdana, sans-serif;
  margin:10px;
}
.stops{
  display:flex;
  justify-content: space-between;
}
.stop {
  padding:0px;
  margin:0px;
  font: 14px/1.5 Georgia, Times New Roman, sans-serif;
}
.myheader p  {
  margin: 0;
  font: 14px/1.5 Georgia, Times New Roman, sans-serif;
}
.myli, .myheader {
  overflow: auto;
  width:90%;
}
.myli {
  padding: 15px;
}
.myli:hover, .myheader:hover {
  background: #eee;
  cursor: pointer;
}
.trip {
  font: 14px/1.2 Helvetica
}
</tyle>
"""

# +
import plotly.graph_objects as go
from datetime import datetime, timedelta
    
def visualize_path(solution: RealSolution, html_widget):
    
    #dict to map transport type to color and to accumulator
    color_cycle=["#0e51ed","#ed0000","#030591","#4fdb4b","#028a00"]

    fig = go.Figure()

    #for each section in the path, display it
    
    trips = solution.trips
    
    for idx, trip in enumerate(trips):
        station0, station1, lon_0, lat_0, lon_1, lat_1, ttype, time = \
        trip.station_dep, trip.station_arr, trip.station_dep.longitude, trip.station_dep.latitude,\
        trip.station_arr.longitude, trip.station_arr.latitude, trip.trans_type, trip.duration

        line_name = trip.route_name
        if trip.route_name is None:
            line_name = "Walk"

        fig.add_trace(go.Scattermapbox(mode = "lines",
                                        lon = [lon_0,lon_1],
                                        lat = [lat_0,lat_1],
                                        hovertext=line_name,
                                        hoverinfo="text",
                                        name=line_name,
                                        showlegend=True,
                                        marker = {'color':color_cycle[idx%len(color_cycle)]},
                                        line={'width':4}))

        fig.add_trace(go.Scattermapbox(mode = "markers",
                                        lon = [lon_0],
                                        lat = [lat_0],
                                        hovertext=station0.station_name,
                                        hoverinfo="lon+lat+text",
                                        showlegend=False,
                                        marker = {'size': 20,'color':color_cycle[idx%len(color_cycle)]}))

    #for the last point
    station1=trips[-1].station_arr
    lat_1,lon_1=trips[-1].station_arr.latitude, trips[-1].station_arr.longitude
    ttype=trips[-1].trans_type
    fig.add_trace(go.Scattermapbox(mode = "markers",
                                    lon = [lon_1],
                                    lat = [lat_1],
                                    hovertext=station1.station_name,
                                    hoverinfo="lon+lat+text",
                                    showlegend=False,
                                    marker = {'size': 20,'color':color_cycle[idx%len(color_cycle)]}))
    
    
    fig.update_layout(
        margin ={'l':0,'t':0,'b':0,'r':0},
        mapbox = {
            'center': {'lon': trips[0].station_dep.longitude, 'lat':trips[0].station_dep.latitude },
            'style': "open-street-map",
            'zoom':12})
    
    total_time = (trips[-1].dep_time++timedelta(seconds=trips[-1].duration))-trips[0].dep_time
    
    html_out = '<ul class="myul">'+HTML_HEADER.format(trips[0].station_dep.station_name, trips[-1].station_arr.station_name, total_time, solution.walking_time, solution.confidence, solution.n_transfers)
    for trip in trips:
        html_out+=trip.to_html(HTML_TRIP)
        
    html_out = html_out+"</ul>"
    
    html_widget.value = html_out+CSS_WIDGET

    fig.show()

# +
# %load_ext autoreload
# %autoreload 2

import sys
sys.path.insert(1, '../scripts/')

from graph import *
from denver import Denver
from run import station_A,station_B,station_C,station_G,station_H,station_I,station_Y, table

all_stations = [station_A,station_B,station_C,station_G,station_H,station_I,station_Y]


# +
station_A = Station("A_node", "A", 47.32112, 8.56321)
station_B = Station("B_node", "B", 47.32612, 8.56621)
station_C = Station("C_node", "C", 47.33312, 8.51421)
station_G = Station("G_node", "G", 47.33812, 8.51221)
station_H = Station("H_node", "H", 47.34712, 8.53521)
station_I = Station("I_node", "I", 47.34412, 8.53221)
station_Y = Station("Y_node", "Y", 47.33912, 8.54121)
# station_G = Station("G_node", "Zurich, Rathaus", 47.33812, 8.51221)
# station_H = Station("H_node", "Kilchberg ZH", 47.34712, 8.53521)
# station_I = Station("I_node", "Kilchberg ZH, Kirche", 47.34412, 8.53221)
# station_Y = Station("Y_node", "Zurich, HB", 47.33912, 8.54121)

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

stop_BII_dep = RouteStopDep("B-I_I_node_dep", "B-I_I", station_I, 0, "B-I", 'Zug', 0, None)
stop_BIB_arr = RouteStopArr("B-I_B_node_arr", "B-I_B", station_B, 1, "B-I", 'Zug', travel_times, stop_BII_dep)


walking_time = 50
walking_Y = WalkingStop("Wa_Y_node", "Wa_Y", station_Y, None)
walking_G = WalkingStop("Wa_G_node", "Wa_G", station_G, [(walking_Y, walking_time)])
walking_Y.set_neighbors([(walking_G, walking_time)])

# station_A.set_stops_dep([])
station_A.set_stops_arr([stop_ACA_arr])
station_B.set_stops_dep([stop_ACB_dep, stop_GHB_dep])
station_B.set_stops_arr([stop_ACB_arr, stop_GHB_arr, stop_BIB_arr])
station_C.set_stops_dep([stop_ACC_dep])
# station_C.set_stops_arr([])
station_G.set_stops_dep([walking_G])
station_G.set_stops_arr([stop_GHG_arr, walking_G])
station_H.set_stops_dep([stop_GHH_dep])
station_H.set_stops_arr([stop_HIH_arr])
station_I.set_stops_dep([stop_HII_dep, stop_BII_dep])
# station_I.set_stops_arr([])
station_Y.set_stops_dep([walking_Y])
station_Y.set_stops_arr([walking_Y])

target_arr_time = 10000
my_list = [i for i in range(0, target_arr_time, 30)]
generic_table = (my_list, [Distrib(1) for i in range(len(my_list))])

table_dict = {stop_ACB_arr: generic_table, stop_ACA_arr: generic_table, stop_GHB_arr: generic_table, 
              stop_GHG_arr: generic_table, stop_HIH_arr: generic_table, stop_BIB_arr: generic_table}

table = Timetable(table_dict, target_arr_time)
all_stations = [station_A,station_B,station_C,station_G,station_H,station_I,station_Y]

# +
from datetime import datetime

day = "2011-11-04"

mock_trips1 = [
    #Trip(station_dep, station_arr, trans_type, duration, route_name, dep_time, n_stops_crossed)
    Trip(station_I, station_H, "Bus", 60, "I-H", datetime.fromisoformat(day+'T17:05:00'), 2),
    Trip(station_H, station_G, "Bus", 150, "H-G", datetime.fromisoformat(day+'T17:07:40'), 3),
    Trip(station_G, station_Y, "Walking", 50, None, datetime.fromisoformat(day+'T17:10:10'), 1)
]

mock_trips2 = [
    Trip(station_I, station_B, "Walking", 60, None, datetime.fromisoformat(day+'T17:05:00'), 1),
    Trip(station_B, station_G, "Bus", 50, "H-G", datetime.fromisoformat(day+'T17:09:20'), 2),
    Trip(station_G, station_Y, "Walking", 50, None, datetime.fromisoformat(day+'T17:10:10'), 1)
]

mock_real_solution1 = RealSolution(mock_trips1, [0.7,1], 0.7, 50)
mock_real_solution2 = RealSolution(mock_trips2, [1,1], 1, 110)

mock_solutions = [mock_real_solution1, mock_real_solution2]

# +
import ipywidgets as widgets
import pandas as pd
import datetime
import sys
sys.path.insert(1, '../scripts/')
from denver import Denver

output = widgets.Output()

start=widgets.Dropdown(
    options=all_stations,
    value=station_I
)

end=widgets.Dropdown(
    options=all_stations,
    value=station_Y
)

proba_slider=widgets.FloatSlider(
    continuous_update=False,
    orientation='horizontal',
    value=0.95,
    min=0,
    max=1.0,
    readout=True,
    readout_format='.2f',
    step=0.01
)

date_picker=widgets.DatePicker(
    description='Departure date',
    value = datetime.date(2022,5,25)
)

hour=widgets.Dropdown(
    options=range(7,21),
    value=13,
    description='Hour:',
    disabled=False,
)

minute=widgets.Dropdown(
    options=range(0,60),
    value=0,
    description='Minute:',
    disabled=False,
)

html_output = widgets.HTML(
    value="Fill the fields and click on <b>Run</b> to plan your trip !"
)

#geostops = pd.read_csv("geostops.csv")

def run_button(b):
    #reset display
    output.clear_output()

    #get all value of widget

    confidence=proba_slider.value
    starting_station=start.value
    arrival_station=end.value
    
    weekday=date_picker.value.weekday()
    avoid =[]
    
    arrival_time=pd.to_datetime("%02d:%02d:00"%(hour.value,minute.value))
    
    
    with output:
        #Reverse start and arrival here because Tenet
        #algo = Denver(confidence, arrival_station, starting_station, table, 1, 10000)
        #solutions = algo.denver()
        solutions = mock_solutions
        n_paths = len(solutions)
        if n_paths==1: print('An optimal paths is found!')
        else: print('{} optimal paths are found!'.format(n_paths))
        
        for i,solution in enumerate(solutions[:1]):
            print('\n\nDisplaying solution #', i+1)
            visualize_path(solution, html_output)
        

#initalize button
button = widgets.Button(
    description='RUN',
    button_style='info',
    tooltip='Run Denver'
)
button.on_click(run_button)

#group all widget
widget_by_row =[widgets.HBox([widgets.Label("Min Confidence Level"), proba_slider]),
                widgets.HBox([widgets.Label("Starting station"), start]),
                widgets.HBox([widgets.Label("Ending station"), end]),
                date_picker,
                hour,
                minute,
                button,
                html_output]
#arrange them in a column
all_widget_in_one=widgets.VBox(widget_by_row)
# -

from IPython.display import display
display(all_widget_in_one,output)






