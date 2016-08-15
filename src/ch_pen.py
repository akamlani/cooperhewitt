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
            self.df_pen.columns = [str(s.replace('.','_')) for s in self.df_pen.columns.tolist()]
            self.df_visitorcreated = self.df_pen[self.df_pen.tool_id != 0]
            self.df_wallobjects    = self.df_pen[(self.df_pen.tool_id == 0) & (self.df_pen.refers_to_object_id !=0)]
        # define some metrics
        self.metrics = {}


### Transformations/Feature Engineering
    def transform_raw_data(self, filename):
        df = pd.read_csv(filename)
        # format timestamp into date/time and extract the number of days
        df = self.format_times(df)
        # extract time period date information
        df['month']   = pd.DatetimeIndex(df['created_dateformat_est']).month
        df['year' ]   = pd.DatetimeIndex(df['created_dateformat_est']).year
        df['day'  ]   = pd.DatetimeIndex(df['created_dateformat_est']).day
        df['dow']     = pd.DatetimeIndex(df['created_dateformat_est']).dayofweek
        df['week']    = pd.DatetimeIndex(df['created_dateformat_est']).week
        df['weekend'] = (pd.DatetimeIndex(df['created_dateformat_est']).dayofweek > 4).astype(int)
        df['hour' ]   = pd.DatetimeIndex(df['created_dateformat_est']).hour
        df['quarter'] = pd.DatetimeIndex(df['created_dateformat_est']).quarter
        # save the converted file to a csv
        df.to_csv("./../export/pen_transformed_raw.csv")
        df.to_pickle("./../export/pen_transformed_raw.pkl")

    def format_times(self, df):
        df.loc[: ,'created_dateformat']     = pd.to_datetime(df['created'], unit='s')
        df.loc[: ,'created_date']           = df['created_dateformat'].apply(pd.datetools.normalize_date)
        df.loc[: ,'created_dateformat_utc'] = df['created_dateformat'].dt.tz_localize('UTC')
        df.loc[: ,'created_dateformat_est'] = df['created_dateformat_utc'].dt.tz_convert('US/Eastern')
        df.loc[: ,'created_date_est']       = pd.to_datetime(df['created_dateformat_est'].dt.date)
        return df

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


    def feature_dummies(self):
        # create feature matrix to be used for modeling
        # frame is the pen data with bursty data and location metadata
        frame = self.df_pen_meta_sub
        df_hours = pd.cut(frame.hour, [0,11,15,18,24], labels=['morning', 'afternoon', 'evening', 'closed'])
        df_hours_dum = pd.get_dummies(df_hours, drop_first=False)
        df_quart_dum = pd.get_dummies(frame.quarter, prefix="quarter", drop_first=False)
        # day of week
        df_dow_dum   = pd.get_dummies(frame.dow, prefix="dow", drop_first=False)
        df_dow_map   = {'dow_0':'Mon','dow_1':'Tues','dow_2':'Wed','dow_3':'Thur','dow_4':'Fri','dow_5':'Sat','dow_6':'Sun'}
        df_dow_dum   = df_dow_dum.drop(['dow_5', 'dow_6'], axis=1)
        df_dow_dum.columns = map(lambda x: df_dow_map[x], df_dow_dum.columns)
        # week of month
        df_wom       = frame.day.map(lambda x: (x/7) + 1 if (x/7) < 4 else 4)
        df_wom_dum   = pd.get_dummies(df_wom, prefix='wom', drop_first=False)
        # year
        df_year_dum  = pd.get_dummies(frame.year, prefix='year', drop_first=False)
        # month
        df_month_dum = pd.get_dummies(frame.month, prefix='month', drop_first=False)
        # room ids, floors
        df_rooms_dum = pd.get_dummies(frame['room_id'], prefix='room', drop_first=False)
        df_floor_dum = pd.get_dummies(frame['room_floor'], prefix='floor', drop_first=False)
        # room capacity (based on objects and spots)
        # it looks like some rooms can only two room are capable of supporting a large num objects and therefore spots
        # in this case for a given room => count_objects ~ count_spots
        df_roomcap  = pd.cut(frame['room_count_objects'], [-10000, 0, 500, 1000, 25000, 100000],
                             labels=['room_nometa', 'room_small_cap', 'room_midsize_cap', 'room_large_cap', 'room_xlarge_cap'])
        df_roomcap_dum = pd.get_dummies(df_roomcap, drop_first=False)
        # spot turn-over (some spots change more frequently than others)
        df_spot_freq = pd.cut(frame['spot_count_objects'], [-10000, 0, 500, 1000, 1500, 5000],
                              labels=['spot_nometa', 'spot_constant_freq', 'spot_normal_freq', 'spot_high_freq', 'spot_dynamic_freq'])
        df_spot_freq_dum = pd.get_dummies(df_spot_freq, drop_first=False)
        # create a separate category for burstiness
        df_bursty_dum = pd.get_dummies(frame['bursty'], prefix='burstiness', drop_first=False)
        df_bursty_dum = df_bursty_dum.drop(['burstiness_0.0'], axis = 1)
        df_bursty_dum = df_bursty_dum.rename(columns={'burstiness_1.0': 'is_bursty',
                                                      'burstiness_-9999.0': 'bursty_'})

        # combine the dummies into a single frame
        self.df_dummies   = pd.concat([df_hours_dum, df_dow_dum, df_wom_dum, df_year_dum, df_month_dum, df_quart_dum,
                                       df_rooms_dum, df_floor_dum, df_roomcap_dum, df_spot_freq_dum, df_bursty_dum], axis=1)
        # create single feature matrix
        cols = ['weekend', 'tagged_after_close', 'during_exhibition', 'visitor_drawn', 'meta_store']
        self.df_features = pd.concat([self.df_dummies, frame[cols]], axis=1)
        self.df_features.to_pickle("./../export/penmeta_features.pkl")


    def custom_features(self, df_meta):
        self.df_pen = self.format_times(df_meta)
        # VISITOR DRAWN vs Wall objects
        self.df_visitordrawn = self.df_pen[self.df_pen['tool_id'] != 0]
        self.df_wallobjects  = self.df_pen[self.df_pen['tool_id'] == 0]
        self.df_pen.loc[:, 'visitor_drawn'] = (self.df_pen['tool_id'] != 0).astype(int)
        # Tag Information (Temporal Locations)
        # is the visit_date during the same time as when the tag occurs
        # self.df_pen = TBD self.df_pen.apply(lambda el: self.tr.acquire_location_at_tag(el, df_locations_in), axis=1)

        # Bursty Time Periods (Burstiness)
        # This should be dealt with for a particular bundle indication of the type of visitor
        self.create_bundle_daily_sequences()
        # Subset data to be utilized and imputate
        self.create_meta_tag()
        # create feature matrix
        self.feature_dummies()

        # ADDITIONAL FEATURES to consider:
        # Weight given to a  bundle based on Frequent Visitor
        # Weight given to an object based if influencer
        # Models: Feature Importance based on if occured during exhibitions (as a label)
        # Likelihood Prediction of a tag being associated with an exhibition

        # AVG TAG/HR, AVG TAG/DAY
        # Time Spent at Museum

        # First Item Tagged
        # Was the Tag part of an Influcencer Event (Top 10)
        # Statistical Scan Comparisions: per Floor/Room/Spot in contrast to capacity or turnover

        # Seasonal, Holidays, Events (Free Entrance)

    def _create_closing_time(self, df_in):
        # Saturday corresponds to closing time and is open after hours
        closing_time = ' 18:00:00'
        if df_in.dow == 5: closing_time = ' 21:00:00'
        ts_close = pd.Timestamp(str(df_in["created_date_est"]).split()[0] + closing_time)
        ts_close = ts_close.tz_localize('US/Eastern')
        ts_after_hours = df_in["created_dateformat_est"] > ts_close
        return int(ts_after_hours)

    def _tag_exhibition_check(self, obj_id, exhibition_id, df_exhibitions_in):
        df_exhibit   = df_exhibitions_in[df_exhibitions_in['id']  == str(exhibition_id)]
        df_tag_match = self.df_pen[self.df_pen['refers_to_object_id'] == int(obj_id)]
        self.df_pen.ix[df_tag_match.index, "during_exhibition"] = \
        df_tag_match['created_date_est'].apply(lambda date: (date >= df_exhibit['created.date_start']) &
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
        bundle_wall_objects_daily = wall_objects.groupby(['bundle_id', 'created_date_est'])
        self.df_bundle_sequences = bundle_wall_objects_daily.apply(lambda frames: sorted ( {ts: obj_id for obj_id, ts
                                                                   in zip(frames['refers_to_object_id'],
                                                                          frames['created_dateformat_est'])}.items() ) )
        self.df_bundle_objsequences = self.df_bundle_sequences.apply(lambda seq: map(lambda tup: tup[1], seq))

        # resample every 10 min and classify as bursty based on threshold
        # mva: average ~ 6.8, std ~9.88, median ~10 max: 168.5, threshold decided on +1 STD: ~17
        # mva_bundles = bundles_bursty.sort_values(by='mva').mva
        # mu  = np.mean(bursty_bundle_rst.mva), std = np.std(bursty_bundle_rst.mva), mu + std ~ 17
        bursty_bundle = self.df_bundle_sequences.apply(lambda x: self.tr.estimate_bursty_bundle(x, 17))
        bursty_bundle_rst = bursty_bundle.reset_index()
        self.df_pen_meta = self.df_pen.merge(bursty_bundle_rst, on=['bundle_id', 'created_date_est'], how='left')
        self.df_pen_meta[['bursty', 'mva', 'ntags']] = self.df_pen_meta[['bursty', 'mva', 'ntags']].fillna(-9999)
        #bursty_bundle.sort_values(by='mva').mva.value_counts().sort_index()

    def create_meta_tag(self):
        date_cols = ['day', 'dow', 'hour', 'month', 'quarter', 'weekend', 'year']
        event_cols = ['tagged_after_close', 'during_exhibition', 'visitor_drawn', 'meta_store', 'bursty', 'ntags', 'mva']
        location_cols = ['room_floor', 'room_id', 'room_count_objects', 'room_count_spots', 'spot_id', 'spot_count_objects']
        cols = date_cols + event_cols + location_cols
        self.df_pen_meta_sub = self.tr.imputate(self.df_pen_meta[cols])
        self.df_pen_meta_sub.loc[:, location_cols] = self.df_pen_meta_sub[location_cols].astype(int)


    def create_object_sequences(self):
        # from a given daily sequence visit, what are the connections
        # for each tuple, extract a directed ordered sequence, removing the timestamp for now
        df_bundle_daily_seq_order = self.df_bundle_sequences.apply(lambda x: x[1])

### Metrics
    def calc_metrics(self, df_objects_in):
        frame = self.df_pen
        # metrics
        self.metrics = {}
        exhibitions             = frame.during_exhibition.value_counts()
        station_metrics         = frame['tool_id'].value_counts()
        n_wallobjects_tagged    = station_metrics[0]
        n_visitorobjects_tagged = station_metrics[station_metrics.index !=0].sum()

        n_wallobjects_unique    = len(frame.refers_to_object_id.unique())
        n_bundles_unique        = len(frame.bundle_id.unique())                 #n_visitors
        n_tag_obs               = frame.shape[0]                                #n_tags
        n_obj_meta_delta        = len( set(frame.refers_to_object_id.unique()) - set(df_objects_in.id.unique()) )
        # number of observations we have metadata for
        n_obs_meta_delta        = frame[frame.refers_to_object_id.isin(df_objects_in.id.unique())].shape[0]

        # activity on a bundle
        df_bundleid_scans_perday = frame.groupby(['bundle_id', 'created_date_est'], sort=True).size()
        df_bundleid_scans_perday = df_bundleid_scans_perday.reset_index()
        df_bundleid_scans_perday = df_bundleid_scans_perday.rename(columns={0: 'scans_per_day'})
        n_dailyavgscans          = df_bundleid_scans_perday['scans_per_day'].mean()
        n_dailyavgscanshr        = 0                                            # TBD


        self.metrics = {'n_wall_objects_tagged': n_wallobjects_tagged, 'n_visitor_objects_tagged': n_visitorobjects_tagged}
        self.metrics.update({'n_unique_wallobjects': n_wallobjects_unique, 'n_unique_bundles': n_bundles_unique,
                            'n_tags': n_tag_obs, 'n_objects_meta_delta': n_obj_meta_delta,
                            'nobs_meta_delta': n_obs_meta_delta, 'nobj_meta_delta': n_obj_meta_delta})
        self.metrics.update({'n_during_exhibition_tags': exhibitions[1], 'n_nonexhibition_tags': exhibitions[0]})
        return self.metrics
