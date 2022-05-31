from datetime import timedelta, datetime
from string import Formatter

# https://stackoverflow.com/a/42320260
def strfdelta(tdelta, fmt='{D:02}d {H:02}h {M:02}m {S:02}s', inputtype='timedelta'):
    """Convert a datetime.timedelta object or a regular number to a custom-
    formatted string, just like the stftime() method does for datetime.datetime
    objects.

    The fmt argument allows custom formatting to be specified.  Fields can 
    include seconds, minutes, hours, days, and weeks.  Each field is optional.

    Some examples:
        '{D:02}d {H:02}h {M:02}m {S:02}s' --> '05d 08h 04m 02s' (default)
        '{W}w {D}d {H}:{M:02}:{S:02}'     --> '4w 5d 8:04:02'
        '{D:2}d {H:2}:{M:02}:{S:02}'      --> ' 5d  8:04:02'
        '{H}h {S}s'                       --> '72h 800s'

    The inputtype argument allows tdelta to be a regular number instead of the  
    default, which is a datetime.timedelta object.  Valid inputtype strings: 
        's', 'seconds', 
        'm', 'minutes', 
        'h', 'hours', 
        'd', 'days', 
        'w', 'weeks'
    """
    
    # Convert tdelta to integer seconds.
    if inputtype == 'timedelta':
        remainder = int(tdelta.total_seconds())
    elif inputtype in ['s', 'seconds']:
        remainder = int(tdelta)
    elif inputtype in ['m', 'minutes']:
        remainder = int(tdelta)*60
    elif inputtype in ['h', 'hours']:
        remainder = int(tdelta)*3600
    elif inputtype in ['d', 'days']:
        remainder = int(tdelta)*86400
    elif inputtype in ['w', 'weeks']:
        remainder = int(tdelta)*604800

    f = Formatter()
    desired_fields = [field_tuple[1] for field_tuple in f.parse(fmt)]
    possible_fields = ('W', 'D', 'H', 'M', 'S')
    constants = {'W': 604800, 'D': 86400, 'H': 3600, 'M': 60, 'S': 1}
    values = {}
    if (remainder>constants['D']):
        fmt = '{D:02}d {H:02}h {M:02} min {S:02}s'
    elif (remainder>constants['H']):
        fmt = '{H:02}h {M:02} min {S:02}s'
    elif (remainder>constants['M']):
        fmt = '{M:02} min {S:02}s'
    elif (remainder>=constants['S']):
        fmt = '{S:02}s'
    else :
        fmt = '{S:02}s'
        
    values["S"] = 0
    found_one = False
    for field in possible_fields:
        if field in desired_fields and field in constants:
            obtained_value, remainder = divmod(remainder, constants[field])
            if (obtained_value > 0) or found_one:
                found_one = True
                values[field] = obtained_value
    return f.format(fmt, **values)


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
        duration_tdelta = timedelta(seconds=self.duration)
        dep_time_format = self.dep_time.strftime("%H:%M:%S")
        arr_time_format = (self.dep_time+duration_tdelta).strftime("%H:%M:%S")
        if (self.trans_type=="Walk"):
            html_str = HTML_TRIP_WALK.format(self.trans_type, self.route_name, self.station_dep.station_name, self.station_arr.station_name, dep_time_format, arr_time_format, strfdelta(duration_tdelta))
        else:
            html_str = HTML_TRIP_TRANSPORT.format("", self.route_name, self.station_dep.station_name, self.station_arr.station_name, dep_time_format, arr_time_format, self.n_stops_crossed, strfdelta(duration_tdelta))
        
        return html_str