from __future__ import division
import pandas as pd
import numpy  as np
import os
import utils

from sklearn.preprocessing import scale

### Semantics/Metrics Engine
def explore_clusters_corr(frame, col):
    # look at the correlation across each cluster to a given target column
    tr = utils.Transforms()
    export_path = os.environ['COOPERHEWITT_ROOT'] + "/export/"
    rooms_table = pd.read_pickle(export_path + "rooms_table.pkl")
    cluster_labels = sorted(frame.cluster.unique())

    # order the clusters based on the ones with the highest percentage in a given cluster
    cluster_labels_pcts = dict()
    for label in (cluster_labels):
        sliced = frame[frame.cluster == label]
        pct_obs = sliced.shape[0]/frame.shape[0]
        cluster_labels_pcts.update({label: pct_obs})

    sorted_pct_clusters = sorted(cluster_labels_pcts.items(), key=lambda x: x[1], reverse=True)
    # find the correlations to the given column within a given cluster
    for label, pct in sorted_pct_clusters:
        sliced = frame[frame.cluster == label]
        sliced = sliced.rename(columns=tr.rename_rooms(sliced, rooms_table)[1])
        cluster_correlations  = (sliced.corr()[col].sort_values(ascending=False)[:10])
        print "Cluster Label:{0}, Pct Sample Obs:{1}".format(label, sliced.shape[0]/frame.shape[0] )
        print sorted(dict(cluster_correlations).items(), key=lambda x: x[1], reverse=True); print

def explore_cluster_features(frame, scale_option=False, k=10):
    # not required to scale as we are dealing with all categorical/binary features
    # here we perform an estimation based on z-score for each cluster
    tr = utils.Transforms()
    export_path = os.environ['COOPERHEWITT_ROOT'] + "/export/"
    rooms_table = pd.read_pickle(export_path + "rooms_table.pkl")
    frame       = frame.rename(columns=tr.rename_rooms(frame, rooms_table)[1])

    cluster_map = {}
    features = frame.drop('cluster', axis=1)
    features_map   = zip(range(len(features.columns)), features.columns)
    features_names = list(features.columns)

    X = scale(features.values) if scale_option else features.values
    y = frame.cluster.values
    for cluster in set(y):
        # associate calculated z-score for each feature in a cluster and remove those that are perfectly associated
        cluster_item = dict( zip(features_names, np.mean(X[y==cluster, :], axis=0)) )
        sorted_cluster_item   = sorted( cluster_item.items(), key = lambda x: x[1], reverse=True)
        filtered_cluster_item = filter(lambda x: int(x[1]) != 1, sorted_cluster_item)
        cluster_map[cluster] = filtered_cluster_item[:k]
    return cluster_map

def explore_room_types(k=10, subk=5, query_types=None):
    # poster:   202, 205
    # Sidewall: 206, 213, 202
    # Drawings: 205, 202, 105, 206, 201
    # Prints:   205, 202, 201, 203, 105, 206, 302, 107
    # Textiles: 202, 206, 205, 105
    # Concept Art: 103, 106
    # Staircase Mode: 105,212
    df_queries  = pd.DataFrame()
    export_path = os.environ['COOPERHEWITT_ROOT'] + "/export/"
    if query_types == None:
        query_types = ['Concept art', 'Drawing', 'poster', 'Print', 'Sidewall', 'textile', 'Staircase model']

    tb = pd.read_pickle(export_path + "object_roomtypes_table.pkl")
    for query in query_types:
        select_rooms = tb[tb.type == query].room_name.value_counts()[:subk]
        df_queries   = df_queries.append( {'query':query, 'rooms': select_rooms.to_dict()}, ignore_index=True )
    return df_queries

def explore_room_type(room_name):
    # what types of artwork are associated with a room
    print room_name
    export_path = os.environ['COOPERHEWITT_ROOT'] + "/export/"
    loctype_table = pd.read_pickle(export_path + "object_roomtypes_table.pkl")
    loctype_table[loctype_table['room.name'] == room_name]['type'].value_counts()


#http://brandonrose.org/clustering
#https://www.toptal.com/machine-learning/clustering-algorithms
#http://stackoverflow.com/questions/36484197/printing-principal-features-in-clusters-python
#http://stackoverflow.com/questions/27491197/scikit-learn-finding-the-features-that-contribute-to-each-kmeans-cluster
