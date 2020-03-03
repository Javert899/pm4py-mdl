from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl, succint_stream_to_exploded_stream
from copy import deepcopy
import numpy as np
from sklearn.metrics import pairwise_distances
from scipy.spatial.distance import cosine
import networkx as nx
from networkx.algorithms.community import asyn_lpa_communities
from networkx.algorithms.community import quality

log0 = mdl_importer.apply("example_logs/mdl/order_management.mdl")
log = succint_mdl_to_exploded_mdl.apply(log0)
stream = log.to_dict('r')
nodes = dict()
for ev in stream:
    ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
    id = "event_id="+str(ev2["event_id"])
    activity = "event_activity="+ev2["event_activity"]
    if id not in nodes:
        nodes[id] = len(nodes)
    if activity not in nodes:
        nodes[activity] = len(nodes)
    for col in ev2:
        if not col.startswith("event_"):
            val = ev2[col]
            oid = "object_id="+str(val)
            cla = "class="+str(col)
            if oid not in nodes:
                nodes[oid] = len(nodes)
            if cla not in nodes:
                nodes[cla] = len(nodes)
A = np.zeros((len(nodes), len(nodes)), dtype=np.float32)
for ev in stream:
    ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
    id = "event_id="+str(ev2["event_id"])
    activity = "event_activity="+ev2["event_activity"]
    A[nodes[id], nodes[activity]] = 1
    A[nodes[activity], nodes[id]] = 1
    for col in ev2:
        if not col.startswith("event_"):
            val = ev2[col]
            A[nodes["object_id=" + str(val)], nodes["class=" + str(col)]] = 1
            A[nodes["class=" + str(col)], nodes["object_id=" + str(val)]] = 1
            A[nodes[id], nodes["object_id=" + str(val)]] = 1
            A[nodes["object_id=" + str(val)], nodes[id]] = 1
overall_sum = np.sum(A)
summ = np.sum(A, axis = 1)
R = A / summ[:, np.newaxis]
N = 1
K = 3
threshold = 0.03
while N < 20:
    sum_cos_sim = 0.0
    Mpow = np.linalg.matrix_power(R, N)
    G = nx.convert_matrix.from_numpy_matrix(Mpow)
    """
    G = nx.Graph()
    for j in range(Mpow.shape[0]):
        G.add_node(j)
    for j in range(Mpow.shape[0]):
        vec = list(Mpow[j, :])
        thresh = max(vec) * threshold
        multt = summ[j]/overall_sum
        veci = [z for z in range(len(vec)) if vec[z] > thresh]
        for z in veci:
            G.add_edge(j, z, weight=vec[z]*multt)
    """
    c = list(asyn_lpa_communities(G, weight="weight"))
    print(N, "modularity", quality.modularity(G, c))
    for j in range(Mpow.shape[0]):
        vals = list(Mpow[j, :]*summ[j]/overall_sum)
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
