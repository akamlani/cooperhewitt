import operator
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 75)

import ch_pen as chp
import ch_spark as chs
import ch_collections as chc
import ch_metaobjects as chm

import databases
import graphs

export_path = "./../export/"
df_objects  = pd.read_pickle(export_path + "collection_objects.pkl")

# debug location information
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
