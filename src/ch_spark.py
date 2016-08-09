import pyspark
from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_unixtime, unix_timestamp, datediff, udf, UserDefinedFunction
from pyspark.sql.types import TimestampType

# from datetime import datetime

class DistributedSpark(object):
    def __init__(self, sc):
        self.sc = sc
        self.hive_cxt = HiveContext(sc)
        self.sql_cxt  = SQLContext(sc)
        print "{0}\n{1}\n{2}\n".format(sc.master, self.hive_cxt, self.sql_cxt)
        print sc._conf.getAll()
        #TBD destructor Unpersist memory

### functionality to query and create tables
    def _create_df_table(self, frame, name):
        df = self.sql_cxt.createDataFrame(frame)
        df.printSchema()
        df.registerTempTable(name)
        self.sql_cxt.cacheTable(name)
        return df

    def _query_temporal_data(self):
        # step 1. calculate difference between location time and pen timestamp
        # n_obs => first join causes for each pen entry * num location entries existent
        # TBD!!!: => Note this "first_value" does not give us the proper value => REWORK for GROUPBY COND
        tb = self.sql_cxt.sql("""
            SELECT p.refers_to_object_id, created, first_value(visit_raw) as visit_raw,
            first_value(room_floor) as room_floor, first_value(room_id) as room_id, first_value(spot_id) as spot_id,
            first_value(room_count_objects) as room_count_objects, first_value(room_count_spots) as room_count_spots,
            first_value(spot_count_objects) as spot_count_objects,
            abs(datediff(
                from_utc_timestamp(from_unixtime(created,   "yyyy-MM-dd"), 'US/Eastern'),
                from_utc_timestamp(from_unixtime(first_value(visit_raw), "yyyy-MM-dd"), 'US/Eastern')
            )) as delta
            FROM pen p
            JOIN locations l ON p.refers_to_object_id = l.refers_to_object_id
            GROUP BY p.refers_to_object_id, created
            ORDER BY p.refers_to_object_id, delta ASC
         """)

        tb = tb.withColumn("meta_store", lit(1))
        tb.registerTempTable('temporal')
        self.sql_cxt.cacheTable('temporal')
        return tb

    def _minimize_query(self, tb_meta):
        # step 2. minmize the delta period to the ones we care about
        tb_meas = self.sql_cxt.sql("""
            SELECT refers_to_object_id, visit_raw, MIN(delta) as delta
            FROM temporal
            GROUP BY refers_to_object_id, visit_raw
            ORDER BY refers_to_object_id, delta ASC
        """)

        tb_meas.registerTempTable('temporal_measure')
        self.sql_cxt.cacheTable('temporal_measure')

        # step 3: further optmize on the join to gather the location table from temporal information
        tb_optimized = self.sql_cxt.sql( """
            SELECT t.refers_to_object_id, created, MIN(t.delta) as delta
            FROM temporal_measure tlm
            JOIN temporal t
            ON tlm.refers_to_object_id = t.refers_to_object_id AND tlm.visit_raw = t.visit_raw
            GROUP BY t.refers_to_object_id, created
            ORDER BY t.refers_to_object_id, delta ASC
        """)

        tb_optimized.registerTempTable('temporal_optimized')
        self.sql_cxt.cacheTable('temporal_optimized')
        tb_samples = tb_meta.join(tb_optimized, on=['refers_to_object_id', 'created', 'delta'] , how='inner')
        return tb_meas, tb_optimized, tb_samples

    # samples_frame  = pen.df_pen
    # temporal_frame = meta.df_locations
    def distribute_temporal_query(self, samples_frame, temporal_frame):
        self.df_pen_spk  = self._create_df_table(samples_frame,  'pen')
        self.df_loc_spk  = self._create_df_table(temporal_frame, 'locations')
        self.tb_meta     = self._query_temporal_data()
        self.tb_measures, self.tb_optimized, self.tb_samples = self._minimize_query(self.tb_meta)
        # combine to the original pen data (meta_store indicates if we had object data to integrate)
        self.df_temporal = self.df_pen_spk.join(self.tb_samples, ['refers_to_object_id', 'created'], "left_outer")
        self.df_temporal = self.df_temporal.fillna({'meta_store': 0})
        # pickle file to pandas: alternatively we can store as a json or parquet columnar format
        export_path = "./../export/"
        df_samples = self.df_temporal.toPandas()
        df_samples.to_pickle(export_path + "penmeta_spark.pkl")

    def distribute_temporal_sequences(self):
        pass


#### Format data that did not work on convergence
## pen
# df_pen_spk = df_pen_spk.withColumn("created_dateformat", df_pen_spk.created_dateformat.astype('timestamp'))
# df_pen_spk = df_pen_spk.withColumn("created_dateformat_utc", df_pen_spk.created_dateformat_utc.astype('timestamp'))
# df_pen_spk = df_pen_spk.withColumn("created_dateformat_est", df_pen_spk.created_dateformat_est.astype('timestamp'))
# df_pen_spk = df_pen_spk.withColumn("created_date", df_pen_spk.created_date.astype('timestamp'))
# df_pen_spk = df_pen_spk.withColumn("created_date_est", df_pen_spk.created_date_est.astype('timestamp'))

## locations
# df_loc_spk = df_loc_spk.withColumn("visit_time", df_loc_spk.visit_time.astype('timestamp'))
# df_loc_spk = df_loc_spk.withColumn("visit_date", col('visit_date').astype('timestamp'))
# func =  udf (lambda x: datetime.strptime(str(x), '%Y-%m-%d'), DateType())
# df_loc_spk = df_loc_spk.withColumn('visit_date', (col('visit_date'))
