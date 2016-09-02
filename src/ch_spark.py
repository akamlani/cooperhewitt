import pyspark
from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_unixtime, unix_timestamp, datediff, udf, UserDefinedFunction, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import os

class DistributedSpark(object):
    def __init__(self, sc, debug=False):
        self.export_path = os.environ['COOPERHEWITT_ROOT'] + "/export/"
        self.sc = sc
        # hive requires writable permissions: ~/ephemeral-hdfs/bin/hadoop fs -chmod 777 /tmp/hive
        self.hive_cxt = HiveContext(sc)
        self.sql_cxt  = SQLContext(sc)
        if debug:
            print "{0}\n{1}\n{2}\n".format(sc.master, self.hive_cxt, self.sql_cxt)
            print sc._conf.getAll()
        #TBD destructor Unpersist memory

### functionality to query and create tables
    def _create_df_table(self, schema, frame, name):
        if schema: df = self.hive_cxt.createDataFrame(frame, schema=schema)
        else: df = self.hive_cxt.createDataFrame(frame)
        df.printSchema()
        df.registerTempTable(name)
        self.hive_cxt.cacheTable(name)
        return df

    def _query_temporal_data(self):
        # step 1. create main temporal table
        # n_obs => first join causes for each pen entry * num location entries existent (dependent on time period)
        samples_temporal_tb = self.hive_cxt.sql("""
            SELECT  s.refers_to_object_id, created, visit_raw,
                    room_floor, room_id, room_name,
                    spot_id, spot_name, spot_description,
                    room_count_objects, room_count_spots, spot_count_objects,
                    abs(datediff(
                        from_utc_timestamp(from_unixtime(created,   "yyyy-MM-dd"), 'US/Eastern'),
                        from_utc_timestamp(from_unixtime(visit_raw, "yyyy-MM-dd"), 'US/Eastern')
                    )) as delta
            FROM samples s
            JOIN temporal t
            ON s.refers_to_object_id = t.refers_to_object_id
            ORDER by s.refers_to_object_id, created, delta
        """)
        samples_temporal_tb.registerTempTable('samplestemporal')
        self.hive_cxt.cacheTable('samplestemporal')
        return samples_temporal_tb

    def _minimize_query(self):
        # From the temporal table, we need minimize the location (multiple locations) to the appropriate sample timestamp
        tb_samples = self.hive_cxt.sql("""
            SELECT *
            FROM (
                SELECT *,
                MIN(delta)   OVER ( PARTITION BY refers_to_object_id, created) AS min_delta,
                row_number() OVER ( PARTITION BY refers_to_object_id, created) AS ranks
                FROM samplestemporal st
                ORDER BY refers_to_object_id
            ) query
            where query.ranks = 1
        """)
        tb_samples = tb_samples.withColumn("meta_store", lit(1))
        tb_samples.registerTempTable('minimizedsamples')
        self.hive_cxt.cacheTable('minimizedsamples')
        return tb_samples

    def distribute_temporal_query(self, (samples_schema,  samples_frame, samples_name),
                                        (temporal_schema, temporal_frame, temporal_name),
                                        cols):
        self.df_samples       = self._create_df_table(samples_schema,  samples_frame,  samples_name)
        self.df_temporal      = self._create_df_table(temporal_schema, temporal_frame, temporal_name)
        self.tb_meta          = self._query_temporal_data()
        self.tb_meta_min      = self._minimize_query()
        # combine to the original pen data (meta_store indicates if we had object data to integrate)
        self.df_samplesmeta   = self.df_samples.join(self.tb_meta_min, ['refers_to_object_id', 'created'], "left_outer")
        self.df_samplesmeta   = self.df_samplesmeta.fillna({'meta_store': 0})
        self.df_samplesmeta.printSchema()
        # pickle file to pandas: alternatively we can store as a json or parquet columnar format
        dropped_cols = ['delta', 'min_delta', 'ranks'] + cols
        samplesmeta_pd  = self.df_samplesmeta.toPandas()
        samplesmeta_pd  = samplesmeta_pd.drop(dropped_cols, axis=1)
        samplesmeta_pd.to_pickle(self.export_path + "penmeta_spark.pkl")

if __name__ == "__main__":
    sc = pyspark.SparkContext(appName = "Spark Transformations")
    # objects
    sp   = DistributedSpark(sc)
    pen  = chp.Pen()
    meta = chm.MetaObjectStore()
    # subset frames
    samples_cols = filter(lambda x: isinstance(pen.df_pen[x].iloc[0], datetime.date), pen.df_pen.columns)
    samples_frame = pen.df_pen.drop(samples_cols, axis=1)
    temporal_cols = ['refers_to_object_id', 'visit_raw', 'room_floor', 'room_id', 'room_name',
                     'spot_id', 'spot_name', 'spot_description',
                     'room_count_objects',  'room_count_spots', 'spot_count_objects']
    temporal_frame = meta.df_locations[temporal_cols]
    # schemas: we do not want to infer directly, as it takes quite a bit of time
    schema_samples = StructType([
        StructField("id",                 IntegerType(), True),StructField("tool_id",                IntegerType(), True),
        StructField("bundle_id",          StringType(),  True),StructField("refers_to_object_id",    IntegerType(), True),
        StructField("created",            IntegerType(), True),StructField("month",                  IntegerType(), True),
        StructField("year",               IntegerType(), True),StructField("day",                    IntegerType(), True),
        StructField("dow",                IntegerType(), True),StructField("week",                   IntegerType(), True),
        StructField("weekend",            IntegerType(), True),StructField("hour",                   IntegerType(), True),
        StructField("quarter",            IntegerType(), True),StructField("tagged_after_close",     IntegerType(), True),
        StructField("during_exhibition",  IntegerType(), True),
    ])
    samples_frame_data = samples_frame.values.tolist()
    # perform spark transformations
    distribute_temporal_query( (schema_samples, samples_frame_data, "samples"),
                               ("", temporal_frame, "temporal"),
                               ['visit_raw'])


#### Format data that did not work on timestamp convergence:
# .withColumn(colname, frame.colname.astype('timestamp'))
# .withColumn(colname, col(colname).astype('timestamp'))
