import pandas as pd
import databases
import os

class Pen(object):
    def __init__(self):
        export_path = "./../export/"
        pen_file = export_path + "pen-collected-items.csv"
        self.db_query  = databases.Database()
        if os.path.exists(export_path + "pen_transformed_features.pkl"):
            self.df_pen = pd.read_pickle(export_path + "pen_transformed_features.pkl")

### Transformations/Feature Engineering
    def transform_raw_data(self, filename):
        df = pd.read_csv(filename)
        # format timestamp into date/time and extract the number of days
        df['created.dateformat']     = pd.to_datetime(df.created, unit='s')
        df['created.date']           = df['created.dateformat'].apply(pd.datetools.normalize_date)
        df['created.dateformat.utc'] = df['created.dateformat'].dt.tz_localize('UTC')
        df['created.dateformat.est'] = df['created.dateformat.utc'].dt.tz_convert('US/Eastern')
        df['created.date.est']       = pd.to_datetime(df['created.dateformat.est'].dt.date)
        
        # extract date information
        df['month']   = pd.DatetimeIndex(df['created.dateformat.est']).month
        df['year' ]   = pd.DatetimeIndex(df['created.dateformat.est']).year
        df['day'  ]   = pd.DatetimeIndex(df['created.dateformat.est']).day
        df['dow']     = pd.DatetimeIndex(df['created.dateformat.est']).dayofweek
        df['weekend'] = (pd.DatetimeIndex(df['created.dateformat.est']).dayofweek > 4).astype(int)
        df['hour' ]   = pd.DatetimeIndex(df['created.dateformat.est']).hour
        df['quarter'] = pd.DatetimeIndex(df['created.dateformat.est']).quarter
        # save the converted file to a csv
        df.to_csv("./../export/pen_transformed_raw.csv")
        df.to_pickle("./../export/pen_transformed_raw.pkl")


    def feature_engineer(self, df_exhibitions_in):
        self.df_pen = pd.read_pickle("./../export/pen_transformed_raw.pkl")

        # extract closing time events (evening or testings)
        self.df_pen['tagged_after_close'] = self.df_pen.apply(self._create_closing_time, axis=1)
        # set up tracking for exhibition events
        self.df_pen['during_exhibition']  = 0
        self.df_pen['during_exhibition']  = self.df_pen['during_exhibition'].astype(int)
        df_exhibitions_in['id'].apply(lambda eid: self._tag_during_exhibition(eid, df_exhibitions_in))
        self.df_pen['during_exhibition']  = self.df_pen['during_exhibition'].astype(int)
        # save the converted file to a csv
        self.df_pen.to_csv("./../export/pen_transformed_features.csv")
        self.df_pen.to_pickle("./../export/pen_transformed_features.pkl")

    def _create_closing_time(self, df_in):
        # Saturday corresponds to closing time and is open after hours
        closing_time = ' 18:00:00'
        if df_in.dow == 5: closing_time = ' 21:00:00'
        ts_close = pd.Timestamp(str(df_in["created.date.est"]).split()[0] + closing_time)
        ts_close = ts_close.tz_localize('US/Eastern')
        ts_after_hours = df_in["created.dateformat.est"] > ts_close
        return int(ts_after_hours)
    
    
    def _tag_exhibition_check(self, obj_id, exhibition_id, df_exhibitions_in):
        df_exhibit   = df_exhibitions_in[df_exhibitions_in['id']  == str(exhibition_id)]
        df_tag_match = self.df_pen[self.df_pen['refers_to_object_id'] == int(obj_id)]
        self.df_pen.ix[df_tag_match.index, "during_exhibition"] = \
        df_tag_match['created.date.est'].apply(lambda date: (date >= df_exhibit['created.date_start']) &
                                               (date <  df_exhibit['created.date_end']) ).values
    
    def _tag_during_exhibition(self, exhibition_id, df_exhibitions_in):
        ds_objectids = self.db_query.query_records("exhibition", str(exhibition_id))
        if ds_objectids != None:
            ds_objectids = pd.DataFrame(ds_objectids['objects'])['id']
            # determine if the tag sequence occured during the period of an exhibition
            ds_objectids.apply(lambda obj_id: self._tag_exhibition_check(obj_id, exhibition_id, df_exhibitions_in))

### Linked data
    def create_bundle_daily_sequences(self):
        # for a given daily trip, create a daily bundle sequence for a particular bundle
        wall_objects = self.df_pen[self.df_pen['refers_to_object_id'] != 0]
        bundle_wall_objects_daily = wall_objects.groupby(['bundle_id', 'created.date.est'])
        self.df_bundle_sequences = bundle_wall_objects_daily.apply(lambda frames: sorted ( {ts: obj_id for obj_id, ts
                                                                   in zip(frames['refers_to_object_id'],
                                                                          frames['created.dateformat.est'])}.items() ) )
        self.df_bundle_objsequences = self.df_bundle_sequences.apply(lambda seq: map(lambda tup: tup[1], seq))
    

    def create_object_sequences(self):
        # from a given daily sequence visit, what are the connections
        # for each tuple, extract a directed ordered sequence, removing the timestamp for now
        df_bundle_daily_seq_order = self.df_bundle_sequences.apply(lambda x: x[1])











