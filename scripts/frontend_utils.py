import sys
import time
import plotly.graph_objects as go
from graph  import RealSolution, Timetable
from datetime import datetime, timedelta
from trip import strfdelta

import ipywidgets as widgets
import pandas as pd
import datetime
from denver import Denver

def prettify_seconds(seconds):
    return time.strftime('%H:%M:%S', time.gmtime(seconds))


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
        if trip.route_name=="" and trip.trans_type=="Walk":
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
    
    html_out = '<ul class="myul">'+HTML_HEADER.format(trips[0].station_dep.station_name, trips[-1].station_arr.station_name, strfdelta(total_time), strfdelta(timedelta(seconds=solution.walking_time)), str(round(solution.confidence, 3)*100)+"%", solution.n_transfers)
    for trip in trips:
        html_out+=trip.to_html()
        
    html_out = html_out+"</ul>"
    
    html_widget.value = html_out+CSS_WIDGET

    fig.show()

def get_widgets(stations, table_dict, cleanup):
    station_names = list(stations.keys())
    
    
    output = widgets.Output()
    start=widgets.Combobox(options=station_names, value=station_names[-2], placeholder= 'Type the station name')

    end=widgets.Combobox(options=station_names, value=station_names[-1], placeholder= 'Type the station name')

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
        disabled=False
    )

    html_output = widgets.HTML(value="Fill the fields and click on <b>Run</b> to plan your trip !")
    #initalize button
    button = widgets.Button(
        description='RUN',
        button_style='info',
        tooltip='Run Denver'
    )
    
    def run_button(b):
        #reset display
        output.clear_output()
        cleanup()

        #get all value of widget

        confidence=proba_slider.value
        starting_station= stations[start.value]
        arrival_station=stations[end.value]

        weekday=date_picker.value.weekday()
        avoid =[]

        arrival_time=pd.to_datetime("%02d:%02d:00"%(hour.value,minute.value))

        with output:
            print(starting_station, arrival_station)
            multiple_sols = False
            target_arr_time = 10000
            #Reverse start and arrival here because Tenet
            table = Timetable(table_dict, confidence, arrival_time)
            denver = Denver(confidence, arrival_station, starting_station, table, multiple_sols)
            solutions = denver.run()

            #solutions = mock_solutions
            n_paths = len(solutions)

            for i,solution in enumerate(solutions[:1]):
                visualize_path(solution, html_output)


    button.on_click(run_button)

    #group all widget
    widget_by_row =[widgets.HBox([widgets.Label("Min Confidence Level"), proba_slider]),
                    widgets.Label("Double click to display all stations"),
                    widgets.HBox([widgets.Label("Starting station"), start]),
                    widgets.HBox([widgets.Label("Ending station"), end]),
                    date_picker,
                    widgets.Label("Target arrival time : "),
                    hour,
                    minute,
                    button,
                    html_output]
    
    #arrange them in a column
    all_widget_in_one=widgets.VBox(widget_by_row)
    return all_widget_in_one, output


# format with station dep name, station arr name, total time (nice string), walk time (nice string), success proba, transfers
HTML_HEADER = """
<li class="myheader">
    <p>Your trip from <b>{}</b> to <b>{}</b></p>
    <p>Total time : <b>{}</b> Walking time : <b>{}</b></p>
    <p>Success probability : <b>{}</b></p>
    <p>Transfers : <b>{}</b></p>
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
  font: 14px/1.5 Helvetica, Times New Roman, sans-serif;
}
.myheader p  {
  margin: 0;
  font: 14px/1.5 Helvetica, Times New Roman, sans-serif;
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


