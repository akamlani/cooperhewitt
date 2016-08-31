import operator
import os
import pandas as pd
import cPickle as pickle
from collections import defaultdict

import matplotlib.pyplot as plt
import seaborn as sns

import networkx as nx
import igraph as ig


class GraphData(object):
    def __init__(self):
        pass

class GraphConn(object):
    def __init__(self):
        self.dag = defaultdict(list)
        root_path = os.environ['COOPERHEWITT_ROOT']
        self.export_path = root_path + "/export/"
        self.df_sub_edges = pd.read_pickle(self.export_path  + "community_edges.pkl")
        self.df_sub_nodes = pd.read_pickle(self.export_path  + "community_vertices.pkl")
        #with open(export_path + 'igraph_data.pkl', 'rb') as f: self.gd = pickle.load(f)

    def networkx_build(self, seq):
        # unoptimized implementation: trouble with scaling
        # its possible we have only visited a single object, there is nothing to do
        if not len(seq) > 1: return
        # else process all the nodes
        for idx, vertice in enumerate(seq):
            curr_vertice = self.dag.get(vertice, [])
            if idx+1 != len(seq):
                neighbor = seq[idx+1]
                curr_vertice.append(neighbor)
                self.dag[vertice] = curr_vertice
        # we built a directed graph
        self.nx_dag = nx.DiGraph(self.dag, name='basic')

    def networkx_display(self):
        # Layouts: Spring, Spectral, Random, Shell
        nx.draw(self.nx_dag, cmap = plt.get_cmap('jet'), edge_cmap=plt.cm.Blues,with_labels=False)

    def networkx_linkanalysis(self):
        # pagerank: ranking of the nodes in the graph based on the structure of the incoming links
        self.nx_dag_pr = nx.pagerank(self.nx_dag)
        self.nx_dag_prs = sorted(self.nx_dag_pr.items(), key=operator.itemgetter(1), reverse=True)

    def networkx_metrics(self):
        return {'num nodes':   self.nx_dag.number_of_nodes(),
                'num edges':   self.nx_dag.number_of_edges(),
                'num degrees': self.nx_dag.degree() }

    def iGraph_build(self):
        # build the graph nodes for iGraph (currently subsetted from Spark Graphx)
        edges = self.df_sub_edges
        src = edges.src.unique()
        dst = edges.dst.unique()
        vertices = list(set(src).union(set(dst)))
        self.n_vertices = len(vertices)
        # map the vertices as it requires index to be numeric
        vertices_map = {val: idx for idx,val in enumerate(vertices) }
        self.df_sub_nodes['id'] = pd.Series(vertices).apply(lambda x: int(vertices_map[x]))
        # create a neighbor list
        neighbors = list(map(tuple, edges.values))
        neighbors_mapped = map(lambda p: tuple([vertices_map[pi] for pi in p]), neighbors)
        self.G = ig.Graph(neighbors_mapped)

        #communities = G.community_multilevel().membership
        #n_communities = len(set(community))
        #color_list = sns.color_palette("husl", n_communities)
        #edge.betweenness.community(g, directed=F)
        return neighbors_mapped

    def iGraph_layout(self, neighbors):
        layt=self.G.layout('grid_3d', dim=3)
        # Retrieve X,Y,Z vertice coordinates
        vertices = pd.DataFrame( [(layt[v][0], layt[v][1], layt[v][2]) for v in xrange(self.n_vertices)] )
        Xn,Yn,Zn = vertices[0],vertices[1],vertices[2]
        # Retrieve X,Y,Z edge coordinates
        Xe=Ye=Ze = []
        for (src,dst) in neighbors:
            Xe+=[layt[src][0],layt[dst][0], None] # x-coordinates of edge ends
            Ye+=[layt[src][1],layt[dst][1], None] # y-coordinates of edge ends
            Ze+=[layt[src][2],layt[dst][2], None] # z-coordinates of edge ends
        # Retrieve Labels
        labels = list(self.df_sub_nodes.type)
        group  = list(self.df_sub_nodes.label)
        # set the Graph data
        gx = GraphData()
        gx.labels,gx.group = (labels, group)
        gx.Xe,gx.Ye,gx.Ze = (Xe, Ye, Ze)
        gx.Xn,gx.Yn,gx.Zn = (Xn, Yn, Zn)
        gx.vertices = vertices
        gx.layt = layt
        with open(self.export_path + 'igraph_data.pkl', 'wb') as f: pickle.dump(gx, f)
        return gx
