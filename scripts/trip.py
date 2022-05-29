from datetime import datetime, timedelta

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
        if (self.trans_type=="Walking"):
            prefix = "Walk"
        else:
            prefix = f"Take the line {self.route_name}"
        return prefix+f" from {self.station_dep.station_name} to {self.station_arr.station_name} during {pretify_seconds(self.duration)}. Departure : {self.dep_time} Arrival : {self.dep_time+timedelta(seconds=self.duration)}"
    
    def to_html(self, html_template):
        if (self.trans_type=="Walking"):
            head = "Walk"
        else:
            head=self.route_name
        
        html_str = html_template.format(self.trans_type, self.route_name, self.station_dep.station_name, self.station_arr.station_name, self.dep_time, self.dep_time+timedelta(seconds=self.duration), self.n_stops_crossed, self.duration)
        return html_str