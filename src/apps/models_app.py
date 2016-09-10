import os
import sys
import pandas as pd
import ch_pen as chp


if __name__ == "__main__":
    root_path = os.environ['COOPERHEWITT_ROOT']
    export_path = root_path + "/export/"

    # create temporal features using spark transformations
    pen = chp.Pen()
    df_temporal_meta = pd.read_pickle(export_path + 'penmeta_spark.pkl')
    pen.create_temporal_features(df_temporal_meta)
