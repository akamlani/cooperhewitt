from __future__ import division
import pandas as pd
import numpy as np
import math


conv_dt = lambda x: pd.to_datetime(x, format='%Y-%m-%d')
class Utils(object):
    def __init__(self):
        pass

    def save_json_tofile(data_json, filename):
        with open("./../export/"+filename, 'w') as f:
            print "file api: results type", type(data_json)
            json.dump(data_json, f, indent=4, ensure_ascii=True)

    def load_json_fromfile(filename, kf):
        with open(filename, 'r') as f:
            data_json = json.load(f)
            df = pd.DataFrame(data_json[kf])
        return df

    def clean_duplicates(self, obj_id, df_in, items):
        df_match = df_in[df_in.refers_to_object_id == obj_id]
        entries  = df_match[items]
        entries  = entries.drop_duplicates()
        indices_duplicated = list(set(df_match.index) - set(entries.index))
        return indices_duplicated

class Transforms(object):
    def __init__(self):
        pass

    def transform_locations(self, df_objects_in):
        # location data normalization (object id, location data), as information is nested in object store per id
        df_objid_loc = df_objects_in[['id', 'location_visit']]
        df_objid_loc = df_objid_loc.rename(columns={'id':'refers_to_object_id'}, inplace=False)
        df_objid_loc.refers_to_object_id = df_objid_loc.refers_to_object_id.astype(long)
        df_objid_loc = pd.concat([df_objid_loc.refers_to_object_id,
        pd.io.json.json_normalize( df_objid_loc.location_visit )],axis=1 )
        df_objid_loc = df_objid_loc.rename(columns={'id':'location_id'}, inplace=False)
        df_objid_loc['visit_raw']   = df_objid_loc['visit_time']
        df_objid_loc['visit_time']  = pd.to_datetime(df_objid_loc['visit_time'], unit='s')
        df_objid_loc['visit_date']  = (df_objid_loc['visit_time'].dt.date).astype(np.datetime64)
        return df_objid_loc

    def imputate(self, frame):
        # if objects are not in the metastore, we just set ambiguous large values to indicate test set
        cols = ['room_floor', 'room_id', 'room_count_objects', 'room_count_spots', 'spot_id', 'spot_count_objects']
        cond = (frame.meta_store == 0) & (frame['room_floor'].isnull())
        frame.loc[cond, cols] = -9999
        return frame

    def extract_roomname(self, row):
        # from the name field of a site_spots extract the given room so we can gather a description
        name = map(lambda s: s.strip() , row['name'].split(','))
        room = filter(lambda x: x.isdigit(), name)
        return str(room[0]) if len(room) > 0 else name[-1]

    def rename_rooms(self, frame, mapping_frame):
        # rename rooms according to prefix
        rooms = [s.strip('room_') for s in frame.columns if s.startswith('room') and 'cap' not in s and 'nometa' not in s]
        room_map = {'room_'+rm: 'rm_' + mapping_frame[mapping_frame['id'] == int(rm)].name.values[0] \
                    for rm in rooms if '9999' not in rm}
        return rooms, room_map
