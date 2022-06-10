import pandas as pd
from networkit import *
import tabulate
import networkit as nk
import networkx as nx
import matplotlib.pyplot as plt

# data import
df_uniqueUsers = pd.read_csv("./data/Top_10_Unique_Users.csv", sep=",", index_col=[0])
df_trans_ERC721 = pd.read_csv("./data/Top_10_ERC_721_transfers.csv", sep=",", index_col=[0])

# create list of unqiue Users
list_uniqueUsers = df_uniqueUsers["owner_of"].values.tolist()
print("Number of unique Users:", len(list_uniqueUsers))

# only ERC721 transactions between current PFP holders
df_trans_matched_ERC721 = df_trans_ERC721[df_trans_ERC721.from_address.isin(
    list_uniqueUsers) & df_trans_ERC721.to_address.isin(list_uniqueUsers)]
print("Transactions only between holders:", len(df_trans_matched_ERC721))

# create graph file for networkit
G = nx.from_pandas_edgelist(df_trans_matched_ERC721, source='from_address',
                            target='to_address', edge_attr="value", create_using=nx.MultiGraph())
nx.write_gml(G, "graph_ERC721.gml")


# import graph file
G = nk.readGraph("graph_ERC721.gml", nk.Format.GML)
overview(G)

# plot
dd = sorted(centrality.DegreeCentrality(G).run().scores(), reverse=True)
plt.xscale("log")
plt.xlabel("degree")
plt.yscale("log")
plt.ylabel("number of nodes")
# fig = plt.figure(figsize=(3, 6))
plt.plot(dd)
plt.show()

# community detection & basic measures
communities = community.detectCommunities(G, inspect=True)

# Centrality measures
abc = nk.centrality.ApproxBetweenness(G, epsilon=0.1)
abc.run()
abc.ranking()[:5]

# Eigenvector centrality
ec = nk.centrality.EigenvectorCentrality(G)
ec.run()
ec.ranking()[:5]

# PageRank
pr = nk.centrality.PageRank(G, 1e-6)
pr.run()
pr.ranking()[:5]

sizes = communities.subsetSizes()
sizes.sort(reverse=True)
ax1 = plt.subplot(2, 1, 1)
ax1.set_ylabel("size")
ax1.plot(sizes)

ax2 = plt.subplot(2, 1, 2)
ax2.set_xscale("log")
ax2.set_yscale("log")
ax2.set_ylabel("size")
ax2.plot(sizes)
plt.show()

# ERC20--------
# data import
df_uniqueUsers = pd.read_csv("./data/Top_10_Unique_Users.csv", sep=",", index_col=[0])
df_trans_ERC20 = pd.read_csv("./data/Top_10_ERC_20_transfers.csv", sep=",", index_col=[0])

# create list of unqiue Users
list_uniqueUsers = df_uniqueUsers["owner_of"].values.tolist()
print("Number of unique Users:", len(list_uniqueUsers))

# only ERC721 transactions between current PFP holders
df_trans_matched_ERC20 = df_trans_ERC20[df_trans_ERC20.from_address.isin(
    list_uniqueUsers) & df_trans_ERC20.to_address.isin(list_uniqueUsers)]
print("Transactions only between holders:", len(df_trans_matched_ERC20))

# create graph file for networkit
G = nx.from_pandas_edgelist(df_trans_matched_ERC20, source='from_address',
                            target='to_address', edge_attr="value", create_using=nx.MultiGraph())
nx.write_gml(G, "graph_ERC20.gml")


# import graph file
G = nk.readGraph("graph_ERC20.gml", nk.Format.GML)
overview(G)

# plot
dd = sorted(centrality.DegreeCentrality(G).run().scores(), reverse=True)
plt.xscale("log")
plt.xlabel("degree")
plt.yscale("log")
plt.ylabel("number of nodes")
# fig = plt.figure(figsize=(3, 6))
plt.plot(dd)
plt.show()

# community detection & basic measures
communities = community.detectCommunities(G, inspect=True)

# Centrality measures
abc = nk.centrality.ApproxBetweenness(G, epsilon=0.1)
abc.run()
abc.ranking()[:5]

# Eigenvector centrality
ec = nk.centrality.EigenvectorCentrality(G)
ec.run()
ec.ranking()[:5]

# PageRank
pr = nk.centrality.PageRank(G, 1e-6)
pr.run()
pr.ranking()[:5]

sizes = communities.subsetSizes()
sizes.sort(reverse=True)
ax1 = plt.subplot(2, 1, 1)
ax1.set_ylabel("size")
ax1.plot(sizes)

ax2 = plt.subplot(2, 1, 2)
ax2.set_xscale("log")
ax2.set_yscale("log")
ax2.set_ylabel("size")
ax2.plot(sizes)
plt.show()
