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

export_path = "./../export/"

# save object collection to files for later use
museum = chc.Museum()
df_objects  = museum.site_objects()
df_objects.to_csv(export_path + "collection_objects.csv", encoding='utf-8')
df_objects.to_csv(export_path + "collection_objects.tsv", encoding='utf-8', sep='\t')

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
df_meta = pd.read_pickle(export_path + 'penmeta_spark.pkl')
pen.custom_features(df_meta)
