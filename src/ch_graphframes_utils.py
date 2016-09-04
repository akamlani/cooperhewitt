from __future__ import division
import pyspark
from pyspark.sql.functions import desc, count

import ch_graphframes as cgf
import ch_metaobjects as chm
import ch_pen as chp
import aws_utils
import plots

from pprint import pprint
from IPython.core.display import display

import numpy as np
import pandas as pd
import os


### PAGERANK utility methods
def pagerank_topk(ranks_in, k=10):
    '''retrieve the top 'k' pagerank vertices and associated with metadata'''
    df_rank_slice = ranks_in.vertices.select('id', 'department', 'type', 'pagerank')\
                            .orderBy(desc("pagerank"))\
                            .limit(k)
    df_ranks = df_rank_slice.toPandas().sort_values(by='pagerank', ascending=False)
    df_ranks.id = df_ranks.id.astype(long)
    return df_ranks

def inspect_pagerank(metaframe, selected_ranks):
    '''gather metadata about the selected rank vertices, assumes functioning off of pandas dataframe'''
    # gather information about pageranks
    df = metaframe[metaframe.id.isin(list(selected_ranks.id))] \
                            .merge(selected_ranks, on='id') \
                            .sort_values(by='pagerank', ascending=False)\
                            .reset_index().drop(['index', 'type_y'], axis=1)

    df = df.rename(columns={'type_x': 'type'})
    return df

def measure_pagerank(ranks_in, tag_frame, k=10, tier='top'):
    # PageRank based on incoming links (voting) to determine level of importance
    rank_slice = ranks_in.vertices.select("id", "type", "department", "pagerank").orderBy(desc("pagerank")).toPandas()
    rank_slice.id = rank_slice.id.astype(int)
    if tier == 'top': rank_slice = rank_slice[:k]
    else: rank_slice = rank_slice[-k:]

    rank_slice = rank_slice.rename(columns={'id': 'obj_id'})
    infl_tags = rank_slice.merge(tag_frame, left_on='obj_id', right_on='refers_to_object_id', how='inner')
    # are people tagging the influential artworks: 139952 (~3.8%) tags were part of the top 10 influencers
    n_infltags, n_pct_tags = infl_tags.shape[0], infl_tags.shape[0] / tag_frame.shape[0]
    print "# Influential Tags: {0},\n% of Overall Tags: {1}".format(n_infltags, n_pct_tags)
    # did any of these visit any exhibition
    cond = (infl_tags.during_exhibition == 1)
    n_infl_exhibittags, n_pct_infltags, n_pct_tags = (infl_tags[cond].sort_index().shape[0],
                                                      infl_tags[cond].sort_index().shape[0] / n_infltags,
                                                      infl_tags[cond].sort_index().shape[0] / tag_frame.shape[0])
    print "# Per Influential Exhibition Tags: {0}\n% of Influential Tags: {1}\n% of Overall Tags: {2}"\
          .format(n_infl_exhibittags, n_pct_infltags, n_pct_tags)

    # are people tagging influential objects during the day or after closing
    cond = (infl_tags.tagged_after_close == 1)
    print "# Per Influential After Closing Time Tags: {0}".format(infl_tags[cond].sort_index().shape[0])
    # time periods
    cols = ['year', 'hour', 'quarter', 'weekend']
    for col in cols:
         data = infl_tags[col].value_counts().to_dict()
         data = sorted(data.items(), key=lambda x: x[1], reverse=True)
         print "Per Infuential Time Period: ({0}, cnt)\n{1}".format(str(col), data[:5])
    return (rank_slice, infl_tags)

### PAGERANK EVALUATION
def evaluate_pagerank(ranks_in, meta_frame):
    # pagerank information
    # Top 10 Selected pageranks
    df_ranks_selected = pagerank_topk(ranks_in)
    # Metadata about Top 10 Selected Rankings
    df_meta_ranks = inspect_pagerank(meta_frame, df_ranks_selected)
    return df_ranks_selected, df_meta_ranks

def plot_pagerank_activity(infl_tier1_tags_in):
    influential_months = infl_tier1_tags_in.month.value_counts().sort_index()
    influential_dow    = infl_tier1_tags_in.dow.value_counts().sort_index()
    influential_hours  = infl_tier1_tags_in.hour.value_counts().sort_index()
    influential_hours  = influential_hours[(influential_hours.index <= 21) & (influential_hours.index >= 10)]
    months = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
              7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec" }
    dow   = {0:  "Mon", 1: "Tues", 2: "Wed", 3: "Thurs", 4: "Fri", 5: "Sat", 6: "Sun"}
    hours = {h: str(h) + 'am' if h < 12 else str(h-12) + 'pm' if h != 12 else str(h) +'pm' for h in influential_hours.index}
    influential_months.index = influential_months.index.map(lambda x: months[x])
    influential_dow.index = influential_dow.index.map(lambda x: dow[x])
    influential_hours.index = influential_hours.index.map(lambda x: hours[x])
    params = [
      {'frame': influential_months,     'xlabel': 'Month',       'ylabel': '# Tags',  'rot': 0},
      {'frame': influential_dow,        'xlabel': 'Day of Week', 'ylabel': '# Tags',  'rot': 0},
      {'frame': influential_hours,      'xlabel': 'Hour',        'ylabel': '# Tags',  'rot': 0}
      # TBD:  Type of Artwork, Type of Room, Room Number
    ]

    title_str = 'Tag Activity per Top {0} Influential Artworks'.format(10)
    filename  = os.environ['COOPERHEWITT_ROOT'] + '/plots/pagerank_eda.png'
    dsp.create_subplots( params, (1, 3, (16,6)), filename, title_str)


### COMMUNITY (LPA) Utility Methods
def acquire_majority_clusters(communities_in):
    # based on Top 5 Clusters where majority are
    # 1 large community, 1 small community, several small micro-communitiess
    q = communities_in.select("id", "type", "label").groupBy("label").agg(count("id").alias("count")).orderBy(desc("count"))
    maj_clusters    = communities_in.select("id", "department", "loan", "type", "label")
    maj_clusters    = maj_clusters.join(q.limit(5), on='label').select('id', 'department', 'type', 'loan', 'label')
    df_maj_clusters = maj_clusters.toPandas()
    df_maj_clusters = df_maj_clusters.rename(columns={'id': 'obj_id'})
    df_maj_clusters.obj_id = df_maj_clusters.obj_id.astype(long)
    #n_vertices_clusters = df_maj_clusters.shape[0]
    return maj_clusters, df_maj_clusters

def explore_clusters(df_maj_clusters_in, meta_frame, tag_frame):
    '''explore the clusters'''
    # exhibitions: large cluster evenly split
    # after_close: majority occurs during operational hours
    # loan_state:  large cluster: half occur as part of loan, small cluster: majority visit those on loan
    # departments: Drawings, Prints, & Graphic Design; Product Design & Decorative Arts
    # rooms: large cluster: 202, 205; small cluster: 302
    pen_clusters = tag_frame.merge(df_maj_clusters_in, left_on='refers_to_object_id', right_on='obj_id')
    loctypes_clusters = meta_frame.merge(df_maj_clusters_in, left_on='id', right_on='obj_id')
    df_cluster_exhibitions  = pd.DataFrame(pen_clusters.groupby('label').during_exhibition.value_counts())
    df_cluster_afterclosing = pd.DataFrame(pen_clusters.groupby('label').tagged_after_close.value_counts())
    df_cluster_loanstate    = pd.DataFrame(pen_clusters.groupby('label').loan.value_counts())
    df_cluster_departments  = pd.DataFrame(pen_clusters.groupby('label').department.value_counts())
    df_cluster_types = pd.DataFrame(pen_clusters.groupby('label').type.value_counts()).groupby(level=0).head(5)
    df_cluster_rooms = pd.DataFrame(loctypes_clusters.groupby('label').room_name.value_counts()).groupby(level=0).head(5)
    meta_clusters = {'exhibitions': df_cluster_exhibitions, 'afterclosing': df_cluster_afterclosing,
                     'loanstate':   df_cluster_loanstate,   'depts': df_cluster_departments,
                     'types':       df_cluster_types,       'rooms': df_cluster_rooms}
    return meta_clusters

def explore_cluster_correlations(df_maj_clusters_in):
    # correlations of clusters (unable to attribute to exhibitions due to size: TBD MLlib)
    df_type_dumm = pd.get_dummies(df_maj_clusters_in.type, prefix='type')
    df_dept_dumm = pd.get_dummies(df_maj_clusters_in.department, prefix='dept')
    df_merged    = pd.concat([df_dept_dumm, df_type_dumm, df_maj_clusters], axis=1)
    df_merged    = df_merged.drop(['department', 'type'], axis=1)
    cols = df_dept_dumm.columns.tolist() + df_type_dumm.columns.tolist() + [u'label']#[u'label', u'during_exhibition']
    df = pd.DataFrame(df_merged[cols].groupby('label').corr()).sort()
    return df

### COMMUNITY (LPA) EVALUATION
def evaluate_communities(communities_in, meta_frame, tag_frame):
    maj_clusters, df_maj_clusters = acquire_majority_clusters(communities_in)
    clusters_meta = explore_clusters(df_maj_clusters, meta_frame, tag_frame)
    return maj_clusters, clusters_meta

pd.set_option('display.max_columns', 75)
dsp  = plots.Display()
