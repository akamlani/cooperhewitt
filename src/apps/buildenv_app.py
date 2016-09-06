import os
import sys
import pandas as pd

# setup profile environment
sys.path.append(os.environ['COOPERHEWITT_ROOT'] + '/src')
sys.path.append(os.environ['COOPERHEWITT_ROOT'] + '/src/apps')

import ch_pen as chp
import ch_metaobjects as chm


if __name__ == "__main__":
    root_path = os.environ['COOPERHEWITT_ROOT']
    export_path = root_path + "/export/"

    # collection and save/serialize artwork metadata
    # transform metadata that has been collected, requires mongodb to be running
    meta = chm.MetaObjectStore()
    meta.collect_exhibitions_records()
    meta.attach_meta()

    # tranformation and basic features of raw pen data
    pen = chp.Pen()
    df_exhibitions = pd.read_pickle(export_path + "/temporal_exhibitions.pkl")
    pen.transform_raw_data()
    pen.create_basic_features(df_exhibitions)
    # create a daily journey for each visitor based on the items they tag
    pen.create_bundle_daily_sequences()

    # A. at this point we can initiate graph analysis (src/notebooks/graphs.ipynb)
    # B. at this point we need to perform spark transformations (src/notebooks/sparktransformations.ipynb) on the cluster


    # now using the spark transformations, create temporal features
    # df_temporal_meta = pd.read_pickle(export_path + 'penmeta_spark.pkl')
    # pen.create_temporal_features(df_temporal_meta)
