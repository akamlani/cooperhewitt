import networkx as nx
from collections import defaultdict
import operator

import matplotlib.pyplot as plt
import seaborn as sns


class GraphConn(object):
    def __init__(self):
        self.dag = defaultdict(list)

    def create_adj_basic(self, seq):
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

    def display_network(self):
        # Layouts: Spring, Spectral, Random, Shell
        nx.draw(self.nx_dag, cmap = plt.get_cmap('jet'), edge_cmap=plt.cm.Blues,with_labels=False)

    def linkanalysis(self):
        # pagerank: ranking of the nodes in the graph based on the structure of the incoming links
        self.nx_dag_pr = nx.pagerank(self.nx_dag)
        self.nx_dag_prs = sorted(self.nx_dag_pr.items(), key=operator.itemgetter(1), reverse=True)

    def metrics(self):
        return {'num nodes': self.nx_dag.number_of_nodes(),
                'num edges': self.nx_dag.number_of_edges(),
                'num degrees': self.nx_dag.degree() }
