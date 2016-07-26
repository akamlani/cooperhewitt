import pandas as pd

import ch_pen as chp
import ch_collections as chc
import databases

export_path = "./../export/"
### Museum collections
museum = chc.Museum()
site_json      = museum.site_information()
df_departments = museum.site_departments()
df_exhibitions_acquired = museum.site_exhibitions()

# save object collection to files for later use
df_objects     = museum.site_objects()
df_objects.to_csv(export_path + "collection_objects.csv", encoding='utf-8')
df_objects.to_csv(export_path + "collection_objects.tsv", encoding='utf-8', sep='\t')

# request objects via exhibitions
def objects_per_exhibition(df_exhibitions_in):
    json_records = df_exhibitions_acquired['id'].apply(lambda eid: museum.site_objects_via_exhibition(int(eid),'objects'))
    json_record_seq = json_records[json_records.apply(lambda x: len(x) > 0)]
    json_record_seq = json_record_seq.reset_index().drop('index', axis=1)['id']
    return json_record_seq

json_record_seq = objects_per_exhibition(df_exhibitions_acquired)
json_record_seq.to_csv(export_path + "exhibition_objects.csv")
json_record_seq.to_pickle(export_path + "exhibition_objects.pkl")
db = databases.Database()
db.insert_records(json_record_seq, 'exhibition')

# transform exhibitions
conv_dt = lambda x: pd.to_datetime(x, format='%Y-%m-%d')
def transform_exhibitions(df_exhibitions_in):
    # Format Exhibitions
    # There are some dates that do not have an end-date, it is unclear if this is an ongoing active exhibition
    df_exhibitions_in = df_exhibitions_in[df_exhibitions_in.date_end != "0000-00-00"]
    df_exhibitions_in.is_copy = False
    df_exhibitions_in['created.date_end']   = df_exhibitions_in['date_end'].apply(conv_dt)
    df_exhibitions_in['created.date_start'] = df_exhibitions_in['date_start'].apply(conv_dt)
    df_exhibitions_in['created.time_span']  = \
    df_exhibitions_in['created.date_end'] - df_exhibitions_in['created.date_start']
    return df_exhibitions_in

df_exhibitions = transform_exhibitions(df_exhibitions_acquired)



### Pen data
pen = chp.Pen()
pen.transform_raw_data(export_path + "pen-collected-items.csv")
pen.feature_engineer(df_exhibitions)
