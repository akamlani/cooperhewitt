import databases
import utils
import models

import os
import pandas as pd
import numpy as np


class Pen(object):
    def __init__(self):
        self.root_path = os.environ['COOPERHEWITT_ROOT']
        self.export_path = self.root_path + "/export/"
        self.db_query  = databases.Database()
        if os.path.exists(self.export_path + "pen_transformed_features.pkl"):
            self.df_pen = pd.read_pickle(self.export_path + "pen_transformed_features.pkl")
            # separate visitor-created items vs wall-tagged objects
            self.df_visitorcreated = self.df_pen[self.df_pen.tool_id != 0]
            self.df_wallobjects    = self.df_pen[(self.df_pen.tool_id == 0) & (self.df_pen.refers_to_object_id !=0)]
        # define some metrics
        self.metrics = {}

### Transformations/Feature Engineering
    def transform_raw_data(self):
        filename = self.root_path + "/data/pen_collected_items.csv"
        df = pd.read_csv(filename)
        # format timestamp into date/time and extract the number of days
        df = self._format_times(df)
        # extract time period date information
        df['month']   = pd.DatetimeIndex(df['created_dateformat_est']).month
        df['year' ]   = pd.DatetimeIndex(df['created_dateformat_est']).year
        df['day'  ]   = pd.DatetimeIndex(df['created_dateformat_est']).day
        df['dow']     = pd.DatetimeIndex(df['created_dateformat_est']).dayofweek
        df['week']    = pd.DatetimeIndex(df['created_dateformat_est']).week
        df['weekend'] = (pd.DatetimeIndex(df['created_dateformat_est']).dayofweek > 4).astype(int)
        df['hour' ]   = pd.DatetimeIndex(df['created_dateformat_est']).hour
        df['quarter'] = pd.DatetimeIndex(df['created_dateformat_est']).quarter
        # create visitor vs wall records
        df.loc[:, 'visitor_drawn'] = (df['tool_id'] != 0).astype(int)
        # format and save the converted file to a csv
        df.columns = [str(s.replace('.','_')) for s in df.columns.tolist()]
        df.to_csv(self.export_path    + "/pen_transformed_raw.csv")
        df.to_pickle(self.export_path + "/pen_transformed_raw.pkl")
        return df

    def _format_times(self, df):
        '''reformat the datetimes for a given dataframe'''
        slice_datetime = lambda t: t.strftime('%Y-%m-%d %H')
        df.loc[: ,'created_dateformat']     = pd.to_datetime(df['created'], unit='s')
        df.loc[: ,'created_date']           = df['created_dateformat'].apply(pd.datetools.normalize_date)
        df.loc[: ,'created_dateformat_utc'] = df['created_dateformat'].dt.tz_localize('UTC')
        df.loc[: ,'created_dateformat_est'] = df['created_dateformat_utc'].dt.tz_convert('US/Eastern')
        df.loc[: ,'created_date_est']       = pd.to_datetime(df['created_dateformat_est'].dt.date)
        df.loc[:, 'created_dateformat_hr_utc'] = pd.to_datetime(df['created_dateformat_utc'].map(slice_datetime))
        df.loc[:, 'created_dateformat_hr_utc'] = df['created_dateformat_hr_utc'].dt.tz_localize('UTC')
        df.loc[:, 'created_dateformat_hr_est'] = df['created_dateformat_hr_utc'].dt.tz_convert('US/Eastern')
        return df

    def create_basic_features(self, df_exhibitions_in):
        self.df_pen = pd.read_pickle(self.export_path + "/pen_transformed_raw.pkl")
        # extract closing time events (evening or testings)
        self.df_pen['tagged_after_close'] = self.df_pen.apply(self._create_closing_time, axis=1)
        # set up tracking for exhibition events
        self.df_pen['during_exhibition']  = 0
        self.df_pen['during_exhibition']  = self.df_pen['during_exhibition'].astype(int)
        df_exhibitions_in['id'].apply(lambda eid: self._tag_during_exhibition(eid, df_exhibitions_in))
        self.df_pen['during_exhibition']  = self.df_pen['during_exhibition'].astype(int)
        # save the converted file to a csv
        self.df_pen.to_csv(self.export_path    + "/pen_transformed_features.csv")
        self.df_pen.to_pickle(self.export_path + "/pen_transformed_features.pkl")

    def _create_temporal_featurematrix(self):
        '''create feature matrix to be used for modeling'''
        # integrate and subset features into single frame to begin
        date_cols = ['day', 'dow', 'hour', 'month', 'quarter', 'weekend', 'year']
        event_cols = ['tagged_after_close', 'during_exhibition', 'visitor_drawn', 'meta_store', 'bursty', 'n_obs', 'n_anomalies']
        location_cols = ['room_floor', 'room_id', 'room_count_objects', 'room_count_spots', 'spot_id', 'spot_count_objects']
        cols = date_cols + event_cols + location_cols
        self.df_pen_meta_sub = utils.imputate(self.df_pen_meta[cols])
        self.df_pen_meta_sub.loc[:, location_cols] = self.df_pen_meta_sub[location_cols].astype(int)
        # create timeframe features
        frame = self.df_pen_meta_sub
        df_hours = pd.cut(frame.hour, [0,10,12,17,20,24], labels=['pre-morning', 'morning', 'afternoon', 'evening', 'night'])
        df_hours_dum = pd.get_dummies(df_hours, drop_first=False)
        df_quart_dum = pd.get_dummies(frame.quarter, prefix="quarter", drop_first=False)
        # day of week, dropping Sat/Sun as we already have weekend category associated with this
        df_dow_dum   = pd.get_dummies(frame.dow, prefix="dow", drop_first=False)
        df_dow_map   = {'dow_0':'Mon','dow_1':'Tues','dow_2':'Wed','dow_3':'Thur','dow_4':'Fri','dow_5':'Sat','dow_6':'Sun'}
        df_dow_dum   = df_dow_dum.drop(['dow_5', 'dow_6'], axis=1)
        df_dow_dum.columns = map(lambda x: df_dow_map[x], df_dow_dum.columns)
        df_wom       = frame.day.map(lambda x: (x/7) + 1 if (x/7) < 4 else 4)
        df_wom_dum   = pd.get_dummies(df_wom, prefix='wom', drop_first=False)
        df_year_dum  = pd.get_dummies(frame.year, prefix='year', drop_first=False)
        df_month_dum = pd.get_dummies(frame.month, prefix='month', drop_first=False)
        # room ids, floors
        df_rooms_dum = pd.get_dummies(frame['room_id'],    prefix='room', drop_first=False)
        df_floor_dum = pd.get_dummies(frame['room_floor'], prefix='floor', drop_first=False)
        # room capacity (based on objects and spots)
        # some rooms support the ability of a large number of objects and spot locations (count_objects ~ count_spots)
        room_cnts_index  = frame.room_count_objects.value_counts().sort_index().index.tolist()
        room_cnts_bins   = [-10000, 0] + map(lambda percent: np.percentile(room_cnts_index[1:], percent), [70,85,90,95])
        room_cnts_labels = ['room_nometa', 'room_small_cap', 'room_midsize_cap', 'room_large_cap', 'room_xlarge_cap']
        df_roomcap       = pd.cut(frame['room_count_objects'], room_cnts_bins, labels=room_cnts_labels)
        df_roomcap_dum   = pd.get_dummies(df_roomcap, drop_first=False)
        # spot turn-over (some spots change more frequently than others)
        spot_cnts_index  = frame.spot_count_objects.value_counts().sort_index().index.tolist()
        spot_cnts_bins   = [-10000, 0] + map(lambda percent: np.percentile(spot_cnts_index[1:], percent), [50,90,95,100])
        spot_cnts_labels = ['spot_nometa', 'spot_constant_freq', 'spot_normal_freq', 'spot_high_freq', 'spot_dynamic_freq']
        df_spot_freq     = pd.cut(frame['spot_count_objects'], spot_cnts_bins, labels=spot_cnts_labels)
        df_spot_freq_dum = pd.get_dummies(df_spot_freq, drop_first=False)
        # create a separate category for burstiness
        df_bursty_dum = pd.get_dummies(frame['bursty'], prefix='burstiness', drop_first=False)
        df_bursty_dum = df_bursty_dum.drop(['burstiness_0'], axis = 1)
        df_bursty_dum = df_bursty_dum.rename(columns={'burstiness_1': 'is_bursty'}).astype(int)
        # combine the features + dummies into a single frame
        cols = ['weekend', 'tagged_after_close', 'during_exhibition', 'visitor_drawn', 'meta_store']
        self.df_features   = pd.concat([df_hours_dum, df_dow_dum, df_wom_dum, df_year_dum, df_month_dum, df_quart_dum,
                                      df_rooms_dum, df_floor_dum, df_roomcap_dum, df_spot_freq_dum, df_bursty_dum,
                                      frame[cols]], axis=1)
        self.df_features.to_pickle(self.export_path + "/penmeta_features.pkl")

    def create_temporal_features(self, df_meta):
        # re-format the times from the raw timestamp (based on utc)
        self.df_pen = self._format_times(df_meta)
        # from the tag pen samples, create a daily directed ordered sequence for a given bundle (visitor)
        self.create_bundle_daily_sequences()
        # create burstiness feature based on anomaly detection as outliers
        self._create_burstiness_as_anomalies()
        # create temporal feature matrix
        self._create_temporal_featurematrix()

        # ADDITIONAL FEATURES to consider:
        # Weight given to a  bundle based on Frequent Visitor
        # Weight given to an object based if influencer
        # Possible Feature Importance based on if occured during exhibitions (as a label)
        # Likelihood Prediction of a tag being associated with an exhibition
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
        '''for a given daily journey, create a daily bundle sequence for a particular bundle'''
        # create daily sequences
        bundle_wall_objects_daily = self.df_wallobjects.groupby(['bundle_id', 'created_date_est'])
        self.df_bundle_sequences  = bundle_wall_objects_daily.apply(lambda frames: sorted ( {ts: obj_id for obj_id,
                                                                    ts in zip(frames['refers_to_object_id'],
                                                                              frames['created_dateformat_est'])}.items() ) )
        self.df_bundle_objsequences = self.df_bundle_sequences.apply(lambda seq: map(lambda tup: tup[1], seq))
        self.df_bundle_sequences.to_pickle(self.export_path + "pen_bundle_journeys.pkl")
        self.df_bundle_objsequences.to_pickle(self.export_path + "pen_bundle_obj_journeys.pkl")


    def _create_object_sequences(self):
        # from a given daily sequence visit, what are the connections
        # for each tuple, extract a directed ordered sequence, removing the timestamp for now
        df_bundle_daily_seq_order = self.df_bundle_sequences.apply(lambda x: x[1])

    def _create_burstiness_as_anomalies(self):
        '''determine if a particular visitor is bursty, based on statistical anomaly detection'''
        anomaly_detections = self.df_bundle_sequences.apply(lambda x: models.anomaly_detector(x))
        # assign bursts from anomalies
        bursty_bundles = anomaly_detections.reset_index()
        self.df_pen_meta = self.df_pen.merge(bursty_bundles, on=['bundle_id', 'created_date_est'], how='left')
        self.df_pen_meta[['n_obs', 'n_anomalies']] = self.df_pen_meta[['n_obs', 'n_anomalies']].fillna(-9999)
        self.df_pen_meta['bursty'] = (self.df_pen_meta['n_anomalies'] > 0).astype(int)

### Available Metrics
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
        # average daily activity on a bundle
        df_bundleid_scans_perday = frame.groupby(['bundle_id', 'created_date_est'], sort=True).size()
        df_bundleid_scans_perday = df_bundleid_scans_perday.reset_index()
        df_bundleid_scans_perday = df_bundleid_scans_perday.rename(columns={0: 'scans_per_day'})
        n_daily_avgscans         = int(df_bundleid_scans_perday['scans_per_day'].mean())
        # average hourly daily activity on a bundle
        df_bundleid_scans_perhr = frame.groupby(['bundle_id', 'created_dateformat_hr_est'], sort=True).size()
        df_bundleid_scans_perhr = df_bundleid_scans_perhr.reset_index()
        df_bundleid_scans_perhr = df_bundleid_scans_perhr.rename(columns={0: 'scans_per_hr'})
        n_dailyhr_avgscans       = int(df_bundleid_scans_perhr['scans_per_hr'].mean())

        self.metrics = {'n_wall_objects_tagged': n_wallobjects_tagged, 'n_visitor_objects_tagged': n_visitorobjects_tagged}
        self.metrics.update({'n_unique_wallobjects': n_wallobjects_unique, 'n_unique_bundles': n_bundles_unique,
                             'n_tags': n_tag_obs, 'n_objects_meta_delta': n_obj_meta_delta,
                             'nobs_meta_delta': n_obs_meta_delta})
        self.metrics.update({'n_during_exhibition_tags': exhibitions[1], 'n_nonexhibition_tags': exhibitions[0]})
        self.metrics.update({'n_avgdaily_tags': n_daily_avgscans, 'n_avgdailyhr_tags': n_dailyhr_avgscans})
        return self.metrics


if __name__ == "__main__":
    export_path = os.environ['COOPERHEWITT_ROOT'] + '/export/'
    pen = chp.Pen()
    # perform transformation of features from initial pen data file (if required)
    pen.transform_raw_data()
    # create basic features (if required)
    df_exhibitions = pd.read_pickle(export_path + "/temporal_exhibitions.pkl")
    pen.create_basic_features(df_exhibitions)
    # create custom temporal features (if required)
    df_temporal_meta = pd.read_pickle(export_path + '/penmeta_spark.pkl')
    pen.create_temporal_features(df_temporal_meta)
    df_features = pd.read_pickle(export_path + "/penmeta_features.pkl")
    # calculate some given metrics
    df_objects = pd.read_pickle(export_path + "collection_objects.pkl")
    pen.calc_metrics(df_objects)
    del pen
