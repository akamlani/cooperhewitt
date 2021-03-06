import os
import pandas as pd
import numpy as np
import utils
import databases
import ch_collections as chc
from sklearn.metrics import pairwise


class MetaObjectStore(object):
    def __init__(self):
        root_path = os.environ['COOPERHEWITT_ROOT']
        self.export_path = root_path + "/export/"
        self.db = databases.Database()
        self.museum = chc.Museum()

        files = ["departments.pkl", "temporal_locations.pkl", "temporal_exhibitions.pkl",
                 "rooms_table.pkl", "object_roomtypes_table.pkl", "collection_objects.pkl"]
        # all serialized files must exist
        n_serializedfiles = sum([os.path.exists(self.export_path + filename) for filename in files])
        if len(files) != n_serializedfiles:
            self.attach_meta()
        else:
            self.df_objects      = pd.read_pickle(self.export_path + "collection_objects.pkl")
            self.df_departments  = pd.read_pickle(self.export_path + "departments.pkl")
            self.df_locations    = pd.read_pickle(self.export_path + "temporal_locations.pkl")
            self.df_exhibitions  = pd.read_pickle(self.export_path + "temporal_exhibitions.pkl")
            self.df_rooms_table  = pd.read_pickle(self.export_path + "rooms_table.pkl")
            self.df_objects_loctypes = pd.read_pickle(self.export_path + "object_roomtypes_table.pkl")

    def attach_meta(self):
        # request data
        self._collect_artwork_metadata()
        self.site_json  = self.museum.site_information()
        self.df_departments = self.museum.site_departments()
        self.df_departments.to_pickle(self.export_path + "departments.pkl")
        self.df_exhibitions_acquired = self.museum.site_exhibitions()
        self.df_site_spots = self.museum.site_spots()
        self.df_site_rooms = self.museum.site_rooms()
        self.df_site_rooms.count_spots   = self.df_site_rooms.count_spots.astype(int)
        self.df_site_rooms.count_objects = self.df_site_rooms.count_objects.astype(int)
        # location and exhibition data is temporal dependent, not static
        self.df_locations     = self._clean_temporal_data()
        self.df_locations.to_pickle(self.export_path +   "temporal_locations.pkl")
        self.df_exhibitions   = self._transform_exhibitions(self.df_exhibitions_acquired)
        self.df_exhibitions.to_pickle(self.export_path + "temporal_exhibitions.pkl")
        # lookup tables (note there is a dependency on df_locations)
        self._build_temporal_lookups()
        # additional meta data
        self.df_objects_meta  = self.df_objects[['id','department_id', 'is_loan_object']]
        self.df_objects_meta  = self.df_objects.rename(columns={'id':'refers_to_object_id'})
        #cols = filter(lambda x: self.df_objects_meta[x].dtype == np.dtype('O'), self.df_objects_meta.columns)
        #self.df_objects_meta[cols] = self.df_objects_meta[cols].astype(long)

    def collect_exhibitions_records(self):
        '''request, serialize, and insert into database exhibition records'''
        df_exhibitions_acquired = self.museum.site_exhibitions()
        json_record_seq = self._collect_exhibition_artworks(df_exhibitions_acquired)
        json_record_seq.to_csv(self.export_path + "exhibition_objects.csv")
        json_record_seq.to_pickle(self.export_path + "exhibition_objects.pkl")
        self.db.insert_records(list(json_record_seq), 'exhibition')
        return json_record_seq

    def _create_exhibitions_records(self):
        '''insert exhibitions recrods without requesting and serializing'''
        json_record_seq = pd.read_pickle(self.export_path + "exhibition_objects.pkl")
        self.db.insert_records(list(json_record_seq), 'exhibition')

    def _collect_exhibition_artworks(self, df_exhibitions_in):
        '''request information about each particular exhibition'''
        json_records = df_exhibitions_in['id'].apply(lambda eid: self.museum.site_objects_via_exhibition(int(eid),'objects'))
        json_record_seq = json_records[json_records.apply(lambda x: len(x) > 0)]
        json_record_seq = json_record_seq.reset_index().drop('index', axis=1)['id']
        return json_record_seq

    def _collect_artwork_metadata(self):
        '''collect artwork metadata from the museum'''
        self.df_objects    = self.museum.site_objects()
        self.df_objects.id = self.df_objects.id.astype(int)
        self.df_objects.to_csv(self.export_path + "collection_objects.csv", encoding='utf-8')
        self.df_objects.to_csv(self.export_path + "collection_objects.tsv", encoding='utf-8', sep='\t')
        self.df_objects.to_pickle(self.export_path + "collection_objects.pkl")

    def _build_temporal_lookups(self):
        # acquire descriptions of rooms [id, room_name, floor, room_count_objects, count_spots, description]
        self.df_site_spots['room_name'] = self.df_site_spots.apply(utils.extract_roomname, axis=1)
        df_rooms = self.df_site_rooms.rename(columns={'count_objects': 'room_count_objects'})
        df_rooms.id = df_rooms.id.astype(int)
        df_rooms['description'] = df_rooms.apply(lambda row: \
        self.df_site_spots[self.df_site_spots.room_name == row['name']]['description'].values[0],axis=1)
        df_rooms = df_rooms.sort_values(by=['room_count_objects', 'count_spots'], ascending=False)
        self.df_rooms_table = df_rooms
        self.df_rooms_table.to_pickle(self.export_path + "rooms_table.pkl")
        # acquire descriptions of types per given room [id, type, room.id, room.name, room.floor, spot.id]
        loc_cols = ['refers_to_object_id', 'room_id', 'room_name', 'room_floor',
                    'spot_id', 'spot_name', 'spot_description', 'visit_date']
        loc_sub  = self.df_locations[loc_cols]
        object_cols = ['id', 'type']
        df_objects_types = self.df_objects[object_cols].drop_duplicates()
        df_objects_loctypes = df_objects_types.merge(loc_sub, left_on='id', right_on='refers_to_object_id', how='inner')
        df_objects_loctypes = df_objects_loctypes.drop('refers_to_object_id', axis=1)
        df_objects_loctypes = df_objects_loctypes.rename(columns={'spot_description': 'description'})
        df_objects_loctypes = df_objects_loctypes.sort_values(by='id').reset_index().drop('index', axis=1)
        self.df_objects_loctypes = df_objects_loctypes
        self.df_objects_loctypes.to_pickle(self.export_path + "object_roomtypes_table.pkl")

    def _clean_temporal_data(self):
        # location data has temporal data that is duplicated and should not be existent
        items = ['visit_date', 'spot.room_id', 'spot.id']
        df_loc = utils.transform_locations(self.df_objects)
        df_objid_dup = pd.Series(df_loc[df_loc.refers_to_object_id.duplicated()==1]['refers_to_object_id'].unique())
        df_cleaned = df_objid_dup.apply(lambda x: utils.clean_duplicates(x, df_loc, items))
        indices_duplicated = reduce(lambda x,y: x+y, df_cleaned)
        df_loc_cleaned = df_loc.drop(indices_duplicated, axis=0).reset_index()
        df_loc_cleaned = df_loc_cleaned.drop('index', axis=1)
        # renaming columns that are a SQL nightmare
        cat_cols = ['spot.name', 'spot.description', 'site.name', 'room.name', 'room.floor_name']
        cols = filter(lambda x: df_loc_cleaned[x].dtype == np.dtype('O') and x not in cat_cols, df_loc_cleaned.columns)
        df_loc_cleaned[cols]   = df_loc_cleaned[cols].astype(long)
        df_loc_cleaned.columns = [str(s.replace('.','_')) for s in df_loc_cleaned.columns.tolist()]
        return df_loc_cleaned

    def _transform_exhibitions(self, df_exhibitions_in):
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
