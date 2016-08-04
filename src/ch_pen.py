import pandas as pd
import databases
import os
import utils

class Pen(object):
    def __init__(self):
        export_path = "./../export/"
        pen_file = export_path + "pen-collected-items.csv"
        self.db_query  = databases.Database()
        self.tr        = utils.Transforms()
        if os.path.exists(export_path + "pen_transformed_features.pkl"):
            self.df_pen = pd.read_pickle(export_path + "pen_transformed_features.pkl")
        # define some metrics
        self.metrics = {}

### Transformations/Feature Engineering
    def transform_raw_data(self, filename):
        df = pd.read_csv(filename)
        # format timestamp into date/time and extract the number of days
        df['created.dateformat']     = pd.to_datetime(df.created, unit='s')
        df['created.date']           = df['created.dateformat'].apply(pd.datetools.normalize_date)
        df['created.dateformat.utc'] = df['created.dateformat'].dt.tz_localize('UTC')
        df['created.dateformat.est'] = df['created.dateformat.utc'].dt.tz_convert('US/Eastern')
        df['created.date.est']       = pd.to_datetime(df['created.dateformat.est'].dt.date)
        
        # extract time period date information
        df['month']   = pd.DatetimeIndex(df['created.dateformat.est']).month
        df['year' ]   = pd.DatetimeIndex(df['created.dateformat.est']).year
        df['day'  ]   = pd.DatetimeIndex(df['created.dateformat.est']).day
        df['dow']     = pd.DatetimeIndex(df['created.dateformat.est']).dayofweek
        df['week']    = pd.DatetimeIndex(df['created.dateformat.est']).week
        df['weekend'] = (pd.DatetimeIndex(df['created.dateformat.est']).dayofweek > 4).astype(int)
        df['hour' ]   = pd.DatetimeIndex(df['created.dateformat.est']).hour
        df['quarter'] = pd.DatetimeIndex(df['created.dateformat.est']).quarter
        # feature engineeer dates
        # map = {'morning',  'afternoon', 'evening'}
        
        
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
        # VISITOR DRAWN vs Wall objects
        self.df_visitordrawn = self.df_pen[self.df_pen['tool_id'] != 0]
        self.df_wallobjects  = self.df_pen[self.df_pen['tool_id'] == 0]
        self.df_pen['visitor_drawn'] = (self.df_pen['tool_id'] != 0).astype(int)


    def custom_features(self, df_locations_in):
        # Tag Information (Temporal Locations)
        # is the visit_date during the same time as when the tag occurs
        self.df_pen = self.df_pen.apply(lambda el: self.tr.acquire_location_at_tag(el, df_locations_in), axis=1)
        # Bursty Time Periods (Burstiness)
        # This should be dealt with for a particular bundle indication of the type of visitor
        self.create_bundle_daily_sequences()
        # Subset data to be utilized and imputate
        self.create_meta_tag()

        # Weight given to a  bundle based on Frequent Visitor
        # Weight given to an object based if influencer
        # Models: Feature Importance based on if occured during exhibitions (as a label)
        # Likelihood Prediction of a tag being associated with an exhibition
        

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
        # !!!IMPORTANT: There are some objects that have a 0 that are not valid (we should drop these
        # for a given daily trip, create a daily bundle sequence for a particular bundle
        wall_objects = self.df_pen[self.df_pen['refers_to_object_id'] != 0]
        bundle_wall_objects_daily = wall_objects.groupby(['bundle_id', 'created.date.est'])
        self.df_bundle_sequences = bundle_wall_objects_daily.apply(lambda frames: sorted ( {ts: obj_id for obj_id, ts
                                                                   in zip(frames['refers_to_object_id'],
                                                                          frames['created.dateformat.est'])}.items() ) )
        self.df_bundle_objsequences = self.df_bundle_sequences.apply(lambda seq: map(lambda tup: tup[1], seq))

        # resample every 10 min and classify as bursty based on threshold
        # mva: average ~ 6.8, std ~9.88, median ~10 max: 168.5, threshold decided on +1 STD: ~17
        # mva_bundles = bundles_bursty.sort_values(by='mva').mva
        # mu  = np.mean(bursty_bundle_rst.mva)
        # std = np.std(bursty_bundle_rst.mva)
        # mu + std ~ 17
        bursty_bundle = self.df_bundle_sequences.apply(lambda x: self.tr.estimate_bursty_bundle(x, 17))
        bursty_bundle_rst = bursty_bundle.reset_index()
        self.df_pen_meta = self.df_pen.merge(bursty_bundle_rst, on=['bundle_id', 'created.date.est'], how='left')
        self.df_pen_meta[['bursty', 'mva', 'ntags']].fillna(-9999, inplace=True)
        bursty_bundle.sort_values(by='mva').mva.value_counts().sort_index()
    
    def create_meta_tag(self):
        date_cols = ['day', 'dow', 'hour', 'month', 'quarter', 'weekend', 'year']
        event_cols = ['tagged_after_close', 'during_exhibition', 'visitor_drawn', 'meta_store', 'bursty', 'ntags', 'mva']
        location_cols = ['room.floor', 'room.id', 'room.count_objects', 'room.count_spots', 'spot.id', 'spot.count_objects']
        cols = date_cols + event_cols + location_cols
        self.df_pen_meta_sub = self.tr.imputate(self.df_pen_meta[cols])
        self.df_pen_meta_sub.loc[:, location_cols] = self.df_pen_meta_sub[location_cols].astype(int)
    
    
    def create_object_sequences(self):
        # from a given daily sequence visit, what are the connections
        # for each tuple, extract a directed ordered sequence, removing the timestamp for now
        df_bundle_daily_seq_order = self.df_bundle_sequences.apply(lambda x: x[1])

### Metrics
    def metrics(self):
        # metrics
        exhibitions = self.df_pen.during_exhibition.value_counts()
        station_metrics         = self.df_pen['tool_id'].value_counts()
        n_wallobjects_tagged    = station_metrics[0]
        n_visitorobjects_tagged = station_metrics[station_metrics.index !=0].sum()
        n_wallobjects_unique    = len(self.df_pen.refers_to_object_id.unique())
        n_bundles_unique        = len(self.df_pen.bundle_id.unique())
        n_tag_obs               = self.df_pen.shape[0]
        n_obj_meta_delta        = len( set(self.df_pen.refers_to_object_id.unique()) - set(df_locations_in.refers_to_object_id.unique()) )
        n_obs_meta_delta        = self.df_pen[self.df_pen.refers_to_object_id.isin(df_locations_in.refers_to_object_id.unique())].shape[0]
        
        self.metrics = {'n_wall_objects_tagged': n_wallobjects_tagged, 'n_visitor_objects_tagged': n_visitorobjects_tagged}
        self.metrics.update({'n_unique_wallobjects': n_wallobjects_unique, 'n_unique_bundles': n_bundles_unique,
                            'n_tags': n_tag_obs, 'n_objects_meta_delta': n_obj_meta_delta, 'nobs_meta_delta': n_obs_meta_delta})
        self.metrics.update({'n_during_exhibition_tags': exhibitions[1], 'n_nonexhibition_tags': exhibitions[0]})





