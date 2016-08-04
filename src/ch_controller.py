import pandas as pd
import multiprocessing
import ch_pen as chp
import ch_collections as chc
import databases
import graphs
import utils


export_path = "./../export/"
df_objects  = pd.read_pickle(export_path + "collection_objects.pkl")
tr = utils.Transforms()

# acquire data
museum = chc.Museum()
site_json  = museum.site_information()
df_departments = museum.site_departments()
df_exhibitions_acquired = museum.site_exhibitions()

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

# pen data
pen = chp.Pen()
pen.transform_raw_data(export_path + "pen_collected_items.csv")
pen.feature_engineer(df_exhibitions)

# handle the custom features
df_locations = tr.transform_locations(df_objects)
pen.custom_features(df_locations)

# 