from datetime import timedelta, datetime

# format with trans type, line number, dep station name, arr station name, dep time, arr time, #stops, duration (nice string)
HTML_TRIP_TRANSPORT = """
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

# format with trans type, line number, dep station name, arr station name, dep time, arr time, duration (nice string)
HTML_TRIP_WALK = """
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
  <p>Duration : {} </p>
</li>

"""

class Trip:
    def __init__(self, station_dep, station_arr, trans_type, duration, route_name, dep_time, n_stops_crossed):
        self.station_dep: "Node" = station_dep
        self.station_arr: "Node" = station_arr
        self.trans_type: str = trans_type
        self.duration: int = duration # In seconds
        self.route_name: str = route_name
        self.dep_time: datetime = dep_time
        self.n_stops_crossed: int = n_stops_crossed
            
    def __str__(self):
        if (self.trans_type=="Walk"):
            prefix = "Walk"
        else:
            prefix = f"Take the line {self.route_name}"
        return prefix+f" from {self.station_dep.station_name} to {self.station_arr.station_name} during {prettify_seconds(self.duration)}. Departure : {self.dep_time} Arrival : {self.dep_time+timedelta(seconds=self.duration)}"
    
    def to_html(self):
        if (self.trans_type=="Walk"):
            html_str = HTML_TRIP_WALK.format(self.trans_type, self.route_name, self.station_dep.station_name, self.station_arr.station_name, self.dep_time, self.dep_time+timedelta(seconds=self.duration), self.duration)
        else:
            html_str = HTML_TRIP_TRANSPORT.format(self.trans_type, self.route_name, self.station_dep.station_name, self.station_arr.station_name, self.dep_time, self.dep_time+timedelta(seconds=self.duration), self.n_stops_crossed, self.duration)
        
        return html_str