import ch_graphframes_utils as cfgu
import ch_graphframes as cgf
import ch_metaobjects as chm
import ch_pen as chp
import aws_utils

from pprint import pprint
from IPython.core.display import display

import pandas as pd
pd.set_option('display.max_columns', 75)


### SPARK CONFIGURATION (currently using spark-submit instead)
def aws_conf():
    # get master url
    aws = aws_utils.AwsConnUtils()
    dns_mapping = aws.get_public_dns()
    url = 'spark://' + str(dns_mapping['public_dns']) + ':7077'
    # set configuration
    conf = pyspark.SparkConf()
    conf.setMaster(url)
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.driver.memory", "8g")
    sc = pyspark.SparkContext(conf=conf, appName = "Cooper Hewitt Graphs")
    print sc._conf.getAll()


if __name__ == "__main__":
    sc = pyspark.SparkContext(appName = "Cooper Hewitt Graphs")
    ### objects
    #dsp  = plots.Display()
    pen  = chp.Pen()
    meta = chm.MetaObjectStore()
    df_associate_rooms = meta.debug_topk_room_types()
    display(df_associate_rooms)

    ### Graph Frames
    sgf = cgf.SparkGraphFrames(sc)
    sgf.create_graph()
    sgf.df_edges.printSchema()
    sgf.df_vertices.printSchema()

    ### Evaluate PageRank
    ranks = sgf.g.pageRank(resetProbability=0.15, tol=0.01)
    df_ranks_selected, df_meta_ranks = cfgu.evaluate_pagerank(ranks, meta.df_objects_loctypes)
    rank_ids = list(df_ranks_selected.id.unique()[:10])
    # evaluate transition locations
    # src type: 'poster', 'Drawing' 'concept art', destination: influential artwork
    query_tr_in  = {'influence': 'dst', 'transition': 'src'}
    df_transitions_in  = sgf.transition_vertice_types(ranks, rank_ids, query_tr_in)
    # src type: influential artwork, destination: 'poster'/'concept art'; 'poster'/'Drawing'
    query_tr_out = {'influence': 'src', 'transition': 'dst'}
    df_transitions_out = sgf.transition_vertice_types(ranks, rank_ids, query_tr_out)
    ### Transitions to and from influential paintings (k = top 10)
    #print "Transitions towards Influential"
    #sgf.show_transitions(df_transitions_in)
    #print "Transitions departing Influential"
    #sgf.show_transitions(df_transitions_out)
    ### Tier of page ranks
    print "Top 10 Tier Pagerank Measurements"
    top_ranks, infl_tier1_tags     = cfgu.measure_pagerank(ranks, pen.df_pen, k=10, tier='top')
    print "\nBottom 10 Tier Pagerank Measurements"
    bottom_ranks, infl_tierk_tags  = cfgu.measure_pagerank(ranks, pen.df_pen, k=10, tier='bottom')
    ### Plot Time Periods
    cfgu.plot_pagerank_activity(infl_tier1_tags)

    ### Evaluate Communities
    communities = sgf.g.labelPropagation(maxIter=50)
    maj_clusters, clusters_meta = cfgu.evaluate_communities(communities, meta.df_objects_loctypes, pen.df_pen)
    sgf.subset_majority_clusters(maj_clusters)

    ### Unused Models (TBD)
    # strongly connected => each vertex is reachable by every other vertex
    # connectedness = sgf.g.connectedComponents()
    # strongly_connectedness = sgf.g.stronglyConnectedComponents(maxIter=25)
    # connectedness.select("id", "title", "type", "department",  "component").orderBy(desc("component"))
    # strongly_connectedness.select("id", "title", "type", "department", "component").orderBy(desc("component"))
    # triangles = sgf.g.triangleCount()

    #sgf.write_parquet(connectedness, "connectedness.parquet")
    #sgf.write_parquet(ranks,         "ranks.parquet")
    #sgf.write_parquet(triangles,     "triangles.parquet")
    #sgf.write_parquet(communities,   "communities.parquet")
    #communities = sgf.read_parquet("communities.parquet")
    #df_vertices = sgf.read_parquet("vertices.parquet")
    #df_edges    = sgf.read_parquet("edges.parquet")

    #sc.stop()
