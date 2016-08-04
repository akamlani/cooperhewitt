from __future__ import division
import pandas as pd
import numpy as np
import math

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
        # location data normalization (object id, location data)
        df_objid_loc = df_objects_in[['id', 'location_visit']]
        df_objid_loc = df_objid_loc.rename(columns={'id':'refers_to_object_id'}, inplace=False)
        df_objid_loc.refers_to_object_id = df_objid_loc.refers_to_object_id.astype(long)
        df_objid_loc = pd.concat([df_objid_loc.refers_to_object_id,
        pd.io.json.json_normalize( df_objid_loc.location_visit )],axis=1 )
        df_objid_loc = df_objid_loc.rename(columns={'id':'location_id'}, inplace=False)
        df_objid_loc['visit_time']  = pd.to_datetime(df_objid_loc['visit_time'], unit='s')
        df_objid_loc['visit_date']  = (df_objid_loc['visit_time'].dt.date).astype(np.datetime64)
        return df_objid_loc

    def imputate(self, frame):
        # if objects are not in the metastore, we just set ambiguous large values to indicate test set
        cols = ['room.floor', 'room.id', 'room.count_objects', 'room.count_spots',
                'spot.id', 'spot.count_objects']
        cond = (frame.meta_store == 0) & (frame['room.floor'].isnull())
        frame.loc[cond, cols] = -9999
        return frame
    

    def acquire_location_at_tag(self, el, df_locations_sub_in):
        # input is the input element pen tag observation and the locations metadata
        # look up the appropriate object id in the location table
        elements = df_locations_sub_in[df_locations_sub_in.refers_to_object_id == el.refers_to_object_id]
        # find the possible dates for this object for different locations
        location_dates = elements.visit_date.drop_duplicates()
        nearest_dates = location_dates.apply(lambda x: abs(x - el['created.date.est']) ).sort_values()
        # there may be some object ids that are not in the meta store
        dp = dict(el)
        dp['meta_store'] = 0
        if location_dates.shape[0] > 0:
            nearest_date_entry  = elements.ix[nearest_dates.index[0], :]
            # subset the appropriate columns
            cols = ['refers_to_object_id',
                    'room.id', 'room.name', 'room.count_objects', 'room.count_spots',
                    'room.floor', 'room.floor_name',
                    'spot.id', 'spot.name', 'spot.description', 'spot.count_objects']
            date_entry = nearest_date_entry[cols]
            # converge into a single series
            dp['meta_store'] = 1
            dp.update(date_entry.to_dict())
        return pd.Series(dp)


    def estimate_bursty_bundle(self, bundle_seq, threshold):
        # bursty cycles according to moving average of a bundle
        cnt = len(bundle_seq)
        if cnt > 1:
            ts = pd.Series(bundle_seq).map(lambda x: x[0])
            ts = pd.Series(1 , index = ts)
            ts_samples = ts.resample('10T').count()
            ts_mva  = ts_samples.rolling(window=2, center=False).mean()
            ts_mva  = ts_mva.fillna(-1)
            max_ts_mva = max(ts_mva)
        else:
            max_ts_mva = -1.0
            bursty     = 0

        seq = {'ntags': cnt, 'mva':max_ts_mva, 'bursty': (1 if max_ts_mva > threshold else 0) }
        return pd.Series(seq)






