import pandas as pd
import numpy as np
import utils
import ch_collections as chc
from sklearn.metrics import pairwise

class MetaObjectStore(object):
    def __init__(self):
        self.export_path = "./../export/"
        self.tr = utils.Transforms()
        self.util = utils.Utils()
        self.museum = chc.Museum()
        self.df_objects     = pd.read_pickle(self.export_path + "collection_objects.pkl")
        self.df_locations   = pd.read_pickle(self.export_path + "temporal_locations.pkl")
        #self.df_exhibitions = pd.read_pickle(self.export_path + "temporal_e.pkl")

    def attach_meta(self):
        # request data
        self.site_json  = self.museum.site_information()
        self.df_departments = self.museum.site_departments()
        self.df_exhibitions_acquired = self.museum.site_exhibitions()
        # location and exhibition data is temporal dependent, not static
        self.df_locations     = self.clean_temporal_data()
        self.df_locations.to_pickle(self.export_path +   "temporal_locations.pkl")
        self.df_exhibitions   = self.transform_exhibitions(self.df_exhibitions_acquired)
        self.df_exhibitions.to_pickle(self.export_path + "temporal_exhibitions.pkl")

        self.df_objects_meta  = self.df_objects[['id','department_id', 'is_loan_object']]
        self.df_objects_meta  = self.df_objects.rename(columns={'id':'refers_to_object_id'})
        #cols = filter(lambda x: self.df_objects_meta[x].dtype == np.dtype('O'), self.df_objects_meta.columns)
        #self.df_objects_meta[cols] = self.df_objects_meta[cols].astype(long)

    def clean_temporal_data(self):
        # location data has temporal data that is duplicated and should not be existent
        items = ['visit_date', 'spot.room_id', 'spot.id']
        df_loc = self.tr.transform_locations(self.df_objects)
        df_objid_dup = pd.Series(df_loc[df_loc.refers_to_object_id.duplicated()==1]['refers_to_object_id'].unique())
        df_cleaned = df_objid_dup.apply(lambda x: self.util.clean_duplicates(x, df_loc, items))
        indices_duplicated = reduce(lambda x,y: x+y, df_cleaned)
        df_loc_cleaned = df_loc.drop(indices_duplicated, axis=0).reset_index()
        df_loc_cleaned = df_loc_cleaned.drop('index', axis=1)

        cat_cols = ['spot.name', 'spot.description', 'site.name', 'room.name', 'room.floor_name']
        cols = filter(lambda x: df_loc_cleaned[x].dtype == np.dtype('O') and x not in cat_cols, df_loc_cleaned.columns)
        df_loc_cleaned[cols]   = df_loc_cleaned[cols].astype(long)
        df_loc_cleaned.columns = [str(s.replace('.','_')) for s in df_loc_cleaned.columns.tolist()]
        return df_loc_cleaned

    def transform_exhibitions(self, df_exhibitions_in):
        # Format Exhibitions
        # There are some dates that do not have an end-date, it is unclear if this is an ongoing active exhibition
        df_exhibitions_in = df_exhibitions_in[df_exhibitions_in.date_end != "0000-00-00"]
        df_exhibitions_in.is_copy = False
        df_exhibitions_in['created.date_end']   = df_exhibitions_in['date_end'].apply(utils.conv_dt)
        df_exhibitions_in['created.date_start'] = df_exhibitions_in['date_start'].apply(utils.conv_dt)
        df_exhibitions_in['created.time_span']  = \
        df_exhibitions_in['created.date_end'] - df_exhibitions_in['created.date_start']
        return df_exhibitions_in

    def create_similarity_matrix(self):
        # create a similarity relationship between objects (artworks)
        # create a similarity matrix between 'refers_to_object_id' labels (the objects)
        matrix = self.df_objects_meta.copy()
        matrix = matrix.set_index('refers_to_object_id')
        self.object_cos_sim = pairwise.cosine_similarity(matrix)
