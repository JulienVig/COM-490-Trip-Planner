# !git lfs pull

# %load_ext autoreload
# %autoreload 2

import sys
sys.path.insert(1, '../scripts/')
from frontend_utils import visualize_path, get_widgets
from graph_init import init_graph
from IPython.display import display

# Create the graph, i.e., instantiate all the objects and link them
# ~4s
stations, timetable, cleanup = init_graph()

all_widget_in_one, output = get_widgets(stations, timetable, cleanup)

display(all_widget_in_one, output)

from datetime import datetime
print(datetime.fromtimestamp(stations['Zürich HB'].arr_time).strftime('%Y-%m-%d %H:%M:%S'))
print(stations['Zürich HB'].arr_time/3600)
print(stations['Uster'].arr_time/3600)

print(datetime.fromtimestamp(1589378700).strftime('%Y-%m-%d %H:%M:%S'))



