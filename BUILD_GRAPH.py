from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl, succint_stream_to_exploded_stream
from copy import deepcopy
import numpy as np
from sklearn.metrics import pairwise_distances
from scipy.spatial.distance import cosine
import networkx as nx
from networkx.algorithms.community import greedy_modularity_communities
from networkx.algorithms.community import quality

log0 = mdl_importer.apply("example_logs/mdl/order_management.mdl")
log = succint_mdl_to_exploded_mdl.apply(log0)
stream = log.to_dict('r')
nodes = set()
for ev in stream:
    ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
    id = "event_id="+str(ev2["event_id"])
    activity = "event_activity="+ev2["event_activity"]
    nodes.add(id)
    nodes.add(activity)
    for col in ev2:
        if not col.startswith("event_"):
            val = ev2[col]
            oid = "object_id="+str(val)
            cla = "class="+str(col)
            nodes.add(oid)
            nodes.add(cla)
nodes = list(nodes)
A = np.zeros((len(nodes), len(nodes)))
for ev in stream:
    ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
    id = "event_id="+str(ev2["event_id"])
    activity = "event_activity="+ev2["event_activity"]
    A[nodes.index(id), nodes.index(activity)] = 1
    A[nodes.index(activity), nodes.index(id)] = 1
    for col in ev2:
        if not col.startswith("event_"):
            val = ev2[col]
            A[nodes.index("object_id=" + str(val)), nodes.index("class=" + str(col))] = 1
            A[nodes.index("class=" + str(col)), nodes.index("object_id=" + str(val))] = 1
            A[nodes.index(id), nodes.index("object_id=" + str(val))] = 1
            A[nodes.index("object_id=" + str(val)), nodes.index(id)] = 1
overall_sum = np.sum(A)
summ = np.sum(A, axis = 1)
R = A / summ[:, np.newaxis]
N = 1
K = 3
while N < 20:
    sum_cos_sim = 0.0
    Mpow = np.linalg.matrix_power(R, N)
    G = nx.Graph()
    for j in range(Mpow.shape[0]):
        G.add_node(j)
    for j in range(Mpow.shape[0]):
        for z in range(Mpow.shape[1]):
            if Mpow[j, z] > 0:
                G.add_edge(j, z, weight=Mpow[j, z])
    c = list(greedy_modularity_communities(G, weight="weight"))
    print(N, "modularity", quality.modularity(G, c))
    for j in range(Mpow.shape[0]):
        vals = list(Mpow[j, :])
        vals = sorted([(i, vals[i]) for i in range(len(vals)) if vals[i] > 0], key=lambda x: x[1], reverse=True)
        idxs = []
        z = 0
        while z < min(K, len(vals)):
            idxs.append(vals[z][0])
            z = z + 1
        idxs.append(j)
        #Mpow_red = np.take(Mpow, [idxs, idxs])
        Mpow_red = np.zeros((len(idxs), len(idxs)))
        z = 0
        while z < len(idxs):
            k = 0
            while k < len(idxs):
                Mpow_red[z, k] = Mpow[idxs[z], idxs[k]]
                k = k + 1
            z = z + 1
        dist_out = np.matrix.mean(np.asmatrix(1 - pairwise_distances(Mpow_red, metric="cosine")))
        #print(N, j, dist_out)
        sum_cos_sim = sum_cos_sim + dist_out
    avg_cos_sim = sum_cos_sim / Mpow.shape[0]
    print(N, "cosine_similarity", avg_cos_sim)
    N = N + 1
print(np.linalg.matrix_power(R, 10))
