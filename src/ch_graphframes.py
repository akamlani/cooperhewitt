from __future__ import division
import cPickle as pickle
import pandas as pd
import numpy as np

import pyspark
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
import graphframes as gf

import ch_metaobjects as chm


class SparkGraphFrames(object):
    def __init__(self, sc):
        self.export_path = "./../export/"
        self.hive_cxt = HiveContext(sc)
        self.sql_cxt  = SQLContext(sc)
        self.meta     = chm.MetaObjectStore()

        # data Sources
        self.export_path = "./../export/"
        self.df_objects  = pd.read_pickle(self.export_path + "collection_objects.pkl")
        self.df_objseq   = pd.read_pickle(self.export_path + "pen_bundle_objseq.pkl")
        self.df_pendata  = pd.read_pickle(self.export_path + "pen_transformed_features.pkl")
        self.df_objids   = pd.Series(self.df_pendata.refers_to_object_id.unique()).map(lambda x: x)

    def write_parquet(self, df, filename):
        '''write a spark datagraph into parquet format'''
        df.write.parquet(self.export_path +  filename)


    def create_graph(self):
        # create graph
        self.bind_vertices()
        self.bind_edges()
        self.g = gf.GraphFrame(self.df_vertices, self.df_edges)
        # write to fs in parquet format
        self.g.vertices.write.format('parquet').mode("overwrite").save(self.export_path + "vertices.parquet")
        self.g.edges.write.format('parquet').mode("overwrite").save(self.export_path + "edges.parquet")

    def bind_vertices(self):
        schema_vertices = StructType([
            StructField("id",            StringType(), True),
            StructField("title",         StringType(), True),
            StructField("date",          StringType(), True),
            StructField("department",    StringType(), True),
            StructField("department_id", StringType(), True),
            StructField("type",          StringType(), True),
            StructField("type_id",       StringType(), True),
            StructField("loan",          StringType(), True),
            StructField("meta_store",    IntegerType(), True)
        ])

        #7783 Unique Vertices
        vertices = self.df_objids.map(self.build_vertices)
        self.df_vertices = self.sql_cxt.createDataFrame(vertices, schema=schema_vertices)

    def build_vertices(self, object_id):
        '''build the vertice dataframe for the graph with metadata'''
        # external data: df_objects, df_departments
        datapoint =  ( str(object_id), "", "", "", "", "", "", "", 0)
        entry = self.meta.df_objects[self.meta.df_objects['id'] == object_id]
        if entry.shape[0] > 0:
            departments = self.meta.df_departments
            dept = departments[departments.id == entry.reset_index().department_id[0]]
            datapoint =  (
                str(object_id),
                entry.reset_index().title[0],
                entry.reset_index().date[0],
                dept.reset_index().name[0] if len(dept) > 0 else '' ,
                dept.reset_index().id[0]   if len(dept) > 0 else '' ,
                entry.reset_index().type[0],
                entry.reset_index().type_id[0],
                str(entry.reset_index().is_loan_object[0]),
                1
            )
        return datapoint

    def find_neighbors(self, it):
        '''find all the neighbors from a list of tuples, each row corresponds to a journey trip'''
        # we are not using this function but instead the inline lambda function
        return [(int(vi), int(it[idx+1])) for idx,vi in enumerate(it) if (idx+1) != len(it)]

    def bind_edges(self):
        '''create a dataframe of edges based on a directed graph of of a journey for a visitor '''
        # create a dataframe of tag sequences for a daily journey of a bundle
        df_seq = self.df_objseq
        df_seq = df_seq.reset_index()
        df_seq = df_seq.rename(columns={0: 'tag_sequence'})
        df_bundle_seq = zip(xrange(len(df_seq)), map(lambda p: [int(pi) for pi in p] , df_seq.tag_sequence) )
        self.df_bundle_seq = self.sql_cxt.createDataFrame(df_bundle_seq, ['daily_seq', 'seq'])
        # create edges: find the neighbors inline, rather than via class method due to serialization
        neighbors = self.df_bundle_seq.map(lambda (group, iterable): \
            [(int(vertex), int(iterable[idx+1])) for idx, vertex in enumerate(iterable) if (idx+1) != len(iterable)]
        )
        neighbors = neighbors.flatMap(lambda x: x).sortBy(lambda x: x)
        df_edges  = self.sql_cxt.createDataFrame(neighbors, ['src', 'dst'])
        df_edges  = df_edges.dropDuplicates()
        # filter out empty ones
        valid_vertices = self.df_vertices.filter('meta_store = 1').select('id')
        valid_vertices = valid_vertices.select(valid_vertices.id.cast('long'))
        df_edges_valid = df_edges.join(valid_vertices, df_edges.src == valid_vertices.id).select('src', 'dst')
        df_edges_valid = df_edges_valid.join(valid_vertices, df_edges_valid.dst == valid_vertices.id).select('src','dst')
        self.df_edges = df_edges_valid

    def show_metrics(self):
        print self.g.inDegrees.orderBy(desc('inDegree')).show()
        print self.g.outDegrees.orderBy(desc('outDegree')).show()
        print self.g.degrees.orderBy(desc('degree')).show()
        print "Num Vertices: ", self.g.vertices.count()
        print "Num Edges: ", self.g.edges.count()
