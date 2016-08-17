import numpy as np
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import scale
from scipy.spatial.distance import cdist, pdist, squareform
from scipy.cluster.hierarchy import linkage, dendrogram, cophenet, fcluster, set_link_color_palette
# cdist = {compute distance between sets of observations}
# pdist = {pairwise distances between observations in same set}

from sklearn import metrics
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics import silhouette_score

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import seaborn as sns
import bokeh.plotting
sns.set_style("darkgrid")
sns.set_context("talk")
sns.set_palette("husl")


def execute_PCA(features, n_comp):
    X_centered = scale(features)
    pca = PCA(n_components=n_comp)
    X_pca = pca.fit_transform(X_centered)

    #print "PCA Components:\n{0}".format(pca_model.components_)
    print "First component: " +  str(pca.explained_variance_ratio_[0])
    print "Second component: " + str(pca.explained_variance_ratio_[1])

    # explained_variance = eigenvalues, components_ = eigenvectors of the covariance matrix
    # The first row of components_ are the direction of maximum variance
    # The entries in explained_variance_ratio_ correspond to the rows of components_
    return pca, X_pca

def scree_plot(pca_model):
    nrows, ncols = (1,2)
    plt_size = (16,5)
    fig, (ax1,ax2) = plt.subplots(nrows, ncols, figsize=plt_size)
    # minimum plot
    ratios = pca_model.explained_variance_ratio_
    ax1.plot(range(1,len(ratios)+1), ratios, 'ro-', linewidth=2)
    ax1.set_xlabel("Principal Component", fontsize=12)
    ax1.set_ylabel("Eigenvalue", fontsize=12)
    # cumulative variance
    cum_var = np.cumsum(ratios)
    ax2.plot(range(len(ratios) + 1), np.insert(cum_var, 0, 0), color = 'r', marker = 'o')
    ax2.bar(range(len(ratios)), ratios, alpha = 0.8)
    ax2.axhline(0.9, color = 'g', linestyle = "--")
    ax2.set_xlabel("Principal Component", fontsize=12)
    ax2.set_ylabel("Variance Explained (%)", fontsize=12)
    # common title
    plt.suptitle("Scree Plot", fontsize=16)
    fig.savefig('../plots/scree_plot.png', dpi=100, bbox_inches="tight")

def evaluate_clustering(features):
    Silhouettes, SSEs = [], []
    for k in range(2,51):
        # init=pca.components_
        km = KMeans(n_clusters=k, init='k-means++', n_init=100)
        km.fit(features)
        SSEs.append(km.score(features))
        Silhouettes.append(silhouette_score(features, km.labels_))
        # n_clusters to maximize silhouette coefficient
    return Silhouettes, SSEs


def execute_clustering(features, k):
    km = KMeans(n_clusters=k, init='k-means++', n_init=100)
    km.fit(features)

    assigned_cluster = km.transform(features).argmin(axis=1)
    centroids        = km.cluster_centers_
    values           = km.cluster_centers_.squeeze()
    labels           = km.labels_
    labels_unique    = np.unique(labels)
    # score          = metrics.silhouette_score(features, labels, metric='euclidean')
    # yhat           = km.predict(data)
    return km
    # silhouette range[-1,1]

def plot_cluster_metrics(silhouette, sse):
    k_range = range(2, 2+len(silhouette))
    nrows, ncols = (1,2)
    plt_size = (18,6)
    fig, (ax1,ax2) = plt.subplots(nrows, ncols, figsize=plt_size)
    ax1.scatter(k_range, silhouette)
    ax1.set_xlabel('k')
    ax1.set_ylabel('silhouette score')
    ax2.scatter(k_range, sse)
    ax2.set_xlabel('k')
    ax2.set_ylabel('sum of squared errors')

    ### Usage
    #Silhouettes, SSEs = models.execute_clustering(features_pca, 10)
    #models.plot_cluster_metrics(Silhouettes, SSEs)

def calc_cluster_metrics(features_pca, centroids):
    # calculate distance from each point to a cluster center
    k_metric  = [cdist(features_pca, centroid, 'euclidean') for centroid in centroids]
    dist      = [np.min(ke,axis=1) for ke in k_metric]
    wcss      = sum((d**2) for d in dist )  # total within-cluster sum of squares
    tss       = sum(pdist(X)**2)/X.shape[0] # total sum of squares
    bss       = tss - wcss                  # between-cluster sum of squares


def plot_clusters(df_features):
    figure = plt.figure(figsize=(16,5))
    pca_model, features_pca = execute_PCA(df_features, 2)
    km = execute_clustering(df_features, 10)
    centroids = km.cluster_centers_
    plt.scatter(x=features_pca[:,0], y=features_pca[:,1], c=km.labels_)
    plt.plot(centroids[:,0],centroids[:,1],'sg',markersize=8)


### hierarchical_clustering
def execute_hierarchical_clustering(data):
    # Begin Hiearchial Clustering
    distxy = squareform(pdist(data, metric='cityblock'))
    Z = linkage(distxy, method='complete')
    cutoff = 0.6*max(Z[:,2])
    c, coph_dists = cophenet(Z, pdist(data))
    print "coph distance metric evaluation", c
    clusters = fcluster(Z, t=cutoff, criterion='distance')
    return Z, cutoff

def plot_dendrogram(Z, cutoff):
    fig = plt.figure(figsize=(8,8))
    set_link_color_palette(["#B061FF", "#61ffff"])
    dendro  = dendrogram(Z,
                         leaf_font_size=12,leaf_rotation=90,
                         show_leaf_counts=True,  # otherwise numbers in brackets are counts
                         show_contracted=True,   # distribution impression of truncated
                         truncate_mode='lastp',  #
                         p=25,                   # shows only last p merged clusters
                         color_threshold=cutoff
                         )
    #d = dict(zip(range(len(df_features.columns)), df_features.columns))
    #mapped_labels = map(lambda x: d[int(x)], dendro['ivl'])
    plt.subplots_adjust(top=.99, bottom=0.5, left=0.05, right=0.99)
    plt.title('Hierarchical Clustering Tag Samples(Truncation=25)', fontweight='bold')
    plt.ylabel('Distance', fontweight='bold')
    plt.xlabel('Sample Index/Cluster Size', fontweight='bold')
    plt.axhline(y=cutoff, c='k')

    for tick in plt.gca().xaxis.get_major_ticks():
                    tick.label.set_fontsize(12)
                    tick.label.set_fontweight('bold')
    for tick in plt.gca().yaxis.get_major_ticks():
                    tick.label.set_fontsize(12)
                    tick.label.set_fontweight('bold')
    fig.savefig('../plots/dendrogram.png', dpi=100, bbox_inches="tight")
