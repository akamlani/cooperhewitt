import os
import numpy as np
import pandas as pd
import plotly as py
import plotly.graph_objs as go
#%matplotlib inline

import ch_pen as chp
import ch_metaobjects as chm
import graphs
import plots

root_path = os.environ['COOPERHEWITT_ROOT']
pen = chp.Pen()
meta = chm.MetaObjectStore()
dsp = plots.Display()
# use community graphs
G = graphs.GraphConn()
neighbors = G.iGraph_build()
gx  = G.iGraph_layout(neighbors)
fig = dsp.plotly_graph(gx, 'cooperhewitt_tagbehavior', root_path + '/plots/cooperhewitt_tagbehavior.png')
# for interactivity
# py.plotly.iplot(fig, filename='cooperhewitt_tagbehavior')
