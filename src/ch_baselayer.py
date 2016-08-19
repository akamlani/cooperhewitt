import numpy as np
import pickle
import pandas as pd
pd.set_option('display.max_columns', 75)

import ch_pen as chp
import ch_spark as chs
import ch_collections as chc
import ch_metaobjects as chm

import databases
import utils
import plots

export_path = "./../export/"

# save object collection to files for later use
museum = chc.Museum()
df_objects  = museum.site_objects()
df_objects.id = df_objects.id.astype(int)
df_objects.to_csv(export_path + "collection_objects.csv", encoding='utf-8')
df_objects.to_csv(export_path + "collection_objects.tsv", encoding='utf-8', sep='\t')
df_objects.to_pickle(export_path + "collection_objects.pkl")

# request objects via exhibitions
def objects_per_exhibition(df_exhibitions_in):
    json_records = df_exhibitions_in['id'].apply(lambda eid: museum.site_objects_via_exhibition(int(eid),'objects'))
    json_record_seq = json_records[json_records.apply(lambda x: len(x) > 0)]
    json_record_seq = json_record_seq.reset_index().drop('index', axis=1)['id']
    return json_record_seq

# transform/clean meta objects
meta = chm.MetaObjectStore()
meta.attach_meta()

json_record_seq = objects_per_exhibition(meta.df_exhibitions)
json_record_seq.to_csv(export_path + "exhibition_objects.csv")
json_record_seq.to_pickle(export_path + "exhibition_objects.pkl")
db = databases.Database()
db.insert_records(json_record_seq, 'exhibition')

# insert into database wo/retrieval
json_record_seq = pd.read_pickle(export_path + "exhibition_objects.pkl")
db = databases.Database()
db.insert_records(list(json_record_seq), 'exhibition')

### Pen data
pen = chp.Pen()
pen.transform_raw_data(export_path + "pen-collected-items.csv")
pen.feature_engineer(meta.df_exhibitions)

# create spark transformations (spark context requires notebook and it needs to be self created for a cluster)
meta = chm.MetaObjectStore()
sp = chs.DistributedSpark(sc)
sp.distribute_temporal_query(pen.df_pen, meta.df_locations)

# read in spark transformations and create feature matrix
df_temporal_meta = pd.read_pickle(export_path + 'penmeta_spark.pkl')
pen.custom_features(df_temporal_meta)
df_features = pd.read_pickle(export_path + 'penmeta_features.pkl')

# correlation heatmap
tr   = utils.Transforms()
dsp = plots.Display()
cols = ['is_bursty', 'visitor_drawn', 'during_exhibition',
        'room_small_cap', 'room_midsize_cap', 'room_large_cap', 'room_xlarge_cap',
        'spot_dynamic_freq', 'spot_high_freq', 'spot_normal_freq', 'spot_constant_freq' ]
df_subset_corr = models.calc_correlations(df_features, 0.09, cols, target='during_exhibition')

mapped_cols = {
    'room_midsize_cap':  'rm_mid_cap', 'room_small_cap':  'rm_sm_cap',
    'room_large_cap':    'rm_lg_cap',  'room_xlarge_cap': 'rm_xl_cap',
    'spot_dynamic_freq': 'spot_dyn_turnover',   'spot_high_freq': 'spot_high_turnover',
    'spot_normal_freq':  'spot_small_turnover', 'spot_constant_freq':  'spot_constant_rate',
    'during_exhibition': 'exhibition_tag',
    'visitor_drawn': 'visitor_created', 'is_bursty': 'treasure_hunter',
    'meta_store': 'meta_available', 'year_2015': 'yr_2015', 'floor_3': '3rd_floor'
}
site_rooms = museum.site_rooms()
site_rooms.id = site_rooms['id'].astype(int)
rooms, room_map = tr.rename_rooms(df_subset_corr, site_rooms)
mapped_cols.update(room_map)
df_subset_corr = df_subset_corr.rename(columns=mapped_cols)
df_subset_corr.index = map(lambda x: mapped_cols[x], df_subset_corr.index)
dsp.plot_heatmap(df_subset_corr, './../plots/heatmap.png')

# modeling (hierarchical clustering)
samples = df_features.sample(frac=0.00275)
X = samples.values
Z, cutoff, clusters = models.execute_hierarchical_clustering(X)
samples['cluster'] = clusters
models.debug_clusters(samples)
#meta.debug_room_types()
