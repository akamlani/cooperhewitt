import operator
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 75)

import matplotlib.pyplot as plt
import seaborn as sns
#%matplotlib inline

import ch_pen as chp
import ch_spark as chs
import ch_collections as chc
import ch_metaobjects as chm

import databases
import graphs
import plots

### Objects/Files
pen = chp.Pen()
meta = chm.MetaObjectStore()

export_path = "./../export/"
df_objects  = pd.read_pickle(export_path + "collection_objects.pkl")
df_meta = pd.read_pickle(export_path + 'penmeta_spark.pkl')

### Requests
site_rooms = meta.museum.site_rooms()

### Metrics

# time based
nscans_per_month   = pen.df_pen.groupby(['month'], sort=True).size()
nscans_per_year    = pen.df_pen.groupby(['year'], sort=True).size()
nscans_per_days    = pen.df_pen.groupby(['day'], sort=True).size()
nscans_per_dow     = pen.df_pen.groupby(['dow'], sort=True).size()
nscans_per_hour    = pen.df_pen.groupby(['hour'], sort=True).size()
nscans_per_quarter = pen.df_pen.groupby(['quarter'], sort=True).size()
nscans_per_exhibit = pen.df_pen.groupby(['during_exhibition'], sort=True).size()
nscans_per_exhibit.index = ['non-exhibitions', 'exhibitions']
# specific cases where there are tags done after hours
nscans_per_close   = pen.df_pen.groupby(['tagged_after_close'], sort=True).size()
nscans_per_close.index   = ['open hours', 'after hours']
# popular floors
nscans_per_floor = df_meta['room_floor'].value_counts()
nscans_per_floor = nscans_per_floor.sort_index()
nscans_per_floor.index = nscans_per_floor.index.astype(int)
nscans_per_floori = nscans_per_floor.sort_values(ascending=False)
# popular rooms
nscans_per_room = meta.df_locations['room_name'].value_counts()
nscans_per_room = nscans_per_room.sort_values(ascending=False)
nscans_per_room.index = nscans_per_room.index.astype(int)
nscans_per_roomi = (nscans_per_room)[:7].sort_values(ascending=False)
# popular spots
nscans_per_spot = meta.df_locations['spot_name'].value_counts()
nscans_per_spot = nscans_per_spot.sort_values(ascending=False)[:7]
nscans_per_spot.index = nscans_per_spot.index.map(lambda x: (''.join(x.split(',')[2:]).strip()  ))
# where to people first go
df_bundle_firstitem = pen.df_bundle_sequences.apply(lambda item: item[0][1])
df_freq_firstitem   = df_bundle_firstitem.value_counts().sort_values(ascending=False)
df_freq_firstitem   = df_freq_firstitem.reset_index()
df_freq_firstitem   = df_freq_firstitem.rename(columns={'index':'id', 0:'counts'})
df_freq_meta = pd.DataFrame(df_freq_firstitem, columns=['id', 'counts'])\
              .merge(df_objects)[['id', 'counts', 'department_id', 'type', 'title']]
df_freq_meta = df_freq_meta.merge(meta.df_locations, right_on='refers_to_object_id', left_on='id')\
              [['id', 'room_name', 'spot_name', 'type', 'type', 'counts']]
nroomscans_first_tag = df_freq_meta.groupby('room_name')['counts'].sum().sort_values(ascending=False)[:7]
# what are the high capacity rooms
site_rooms.count_spots   = site_rooms.count_spots.astype(int)
site_rooms.count_objects = site_rooms.count_objects.astype(int)
df_rooms = site_rooms.sort_values(['count_objects', 'count_spots'], ascending=False)
df_rooms.index = df_rooms.name
nhighcap_rooms = df_rooms['count_objects'][:7]

### Plots (EDA)
dsp = plots.Display()
params = [
#  {'frame': nscans_per_year,     'xlabel': "per year",     'ylabel': '# Tags', "type":  "bar", "transform": "Log"},
#  {'frame': nscans_per_hour,     'xlabel': "per hour",     'ylabel': '# Tags', "limits": (9,20)},
#  {'frame': nscans_per_close,    'xlabel': "after closing hours", 'ylabel': '# Scans', "type": "bar", "transform": "Log"},
  {'frame': nscans_per_floor,     'xlabel': 'Floor #',      'ylabel': '# Tags',       'type': 'bar',  'rot': 0},
  {'frame': nscans_per_roomi,     'xlabel': '# Tags',       'ylabel': 'Room #',       'type': 'hbar', 'rot': 60},
  {'frame': nscans_per_spot,      'xlabel': '# Tags',       'ylabel': 'Room,Spot #',  'type': 'hbar', 'rot':60},
  {'frame': n_highcap_rooms,      'xlabel': '# Artworks',   'ylabel': 'Room #',       'type': 'hbar', 'rot':60},
  {'frame': nroomscans_first_tag, 'xlabel': 'Num Scans First Tag', 'ylabel': 'Room#',  'type': 'hbar', 'rot':60},
  {'frame': nscans_per_exhibit,   'xlabel': 'Exhibitions',  'ylabel': '# Tags', 'type': 'bar'},
]

dsp.create_subplots( params, (3, 3, (14,10)), '../plots/pen_eda_full.png')
dsp.create_subplots( params, (2, 3, (14,7)), '../plots/pen_eda_basic.png')

### Models
pca_model, features_pca = models.execute_PCA(df_features, 10)
models.scree_plot(pca_model)




### debug location information
frame = {room_id: int(list(df_locations[df_locations['room_id'] == room_id]['room_count_objects'].unique())[0])
         for room_id in df_locations['room_id'].unique() }
frame = {spot_id: int(list(df_locations[df_locations['spot_id'] == spot_id]['spot_count_objects'].unique())[0])
         for spot_id in df_locations['spot_id'].unique() }
frame_sorted = sorted(frame.items(), key=operator.itemgetter(1), reverse=True)
print len(df_locations['room_id'].unique())                 #21
print len(df_locations['spot_id'].unique())                 #891
print len(df_locations['room_floor'].unique())              #4
print max(df_locations['room_count_objects'].astype(int))   #1, 45473
print max(df_locations['room_count_spots'].astype(int))     #1, 406
print max(df_locations['spot_count_objects'].astype(int))   #1, 2248
