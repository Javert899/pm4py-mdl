import networkx
from pm4py.objects.log.log import EventLog, Trace, Event
from datetime import datetime
from pm4py.objects.log.util import sorting
from collections import Counter


def apply(df, source_attr, target_attr, type_attr, timestamp_key, reverse=False):
    df[source_attr] = df[source_attr].astype(str)
    df[target_attr] = df[target_attr].astype(str)
    df[type_attr] = df[type_attr].astype(str)

    first_df = df.groupby(source_attr).first().reset_index()
    timestamps = first_df[[timestamp_key, source_attr]].to_dict('r')
    timestamps = {x[source_attr]: x[timestamp_key] for x in timestamps}

    map_types = df[[source_attr, type_attr]].to_dict('r')
    map_types = {x[source_attr]: x[type_attr] for x in map_types if x[type_attr] != 'None'}

    map_source_target = dict(df.groupby([source_attr, target_attr]).size())
    map_source_target = list(map_source_target.keys())

    map_source_target = [(map_types[x] + "=" + x, map_types[y] + "=" + y) for (x, y) in map_source_target if
                         x in map_types and y in map_types]
    map_types = {y + "=" + x: y for x, y in map_types.items()}

    G = networkx.DiGraph()
    for k in map_types:
        G.add_node(k)
    for el in map_source_target:
        if reverse:
            G.add_edge(el[1], el[0])
        else:
            G.add_edge(el[0], el[1])

    conn_comp = sorted(list(networkx.connected_components(networkx.Graph(G))), key=lambda x: len(x), reverse=True)

    return G, conn_comp, timestamps


def describe_graph(G, comp):
    SG = G.subgraph(comp)
    nodes = list(SG.nodes)
    edges = list(SG.edges)
    nodes = [(x, y) for x, y in Counter(n.split("=")[0] for n in nodes).items()]
    for i in range(len(nodes)):
        if nodes[i][1] > 1:
            nodes[i] = (nodes[i][0], "N")
        else:
            nodes[i] = (nodes[i][0], "1")
    source0 = {n[0]: [e for e in edges if e[0].split("=")[0] == n[0]] for n in nodes}
    target0 = {n[0]: [e for e in edges if e[1].split("=")[0] == n[0]] for n in nodes}
    source1 = {x: {} for x in source0}
    target1 = {x: {} for x in target0}
    for x in source0:
        ik = set(y[0] for y in source0[x])
        for k in ik:
            source1[x][k.split("=")[1]] = Counter(y[1].split("=")[0] for y in source0[x] if y[0] == k)
    for x in target0:
        ik = set(y[1] for y in target0[x])
        for k in ik:
            target1[x][k.split("=")[1]] = Counter(y[0].split("=")[0] for y in source0[x] if y[1] == k)
    print(source1)
    print(target1)
    # print(edges)


def create_log(G, conn_comp, timestamps, max_comp_len=50, include_loops=False):
    log = EventLog()
    for i in range(len(conn_comp)):
        if len(conn_comp[i]) <= max_comp_len:
            trace = Trace()
            trace.attributes["concept:name"] = str(i)
            SG = G.subgraph(conn_comp[i])
            SGG = networkx.DiGraph(SG)
            edges = list(SGG.edges)
            for e in edges:
                if e[0] == e[1]:
                    SGG.remove_edge(e[0], e[1])
            sorted_nodes = list(networkx.topological_sort(SGG))
            for n in sorted_nodes:
                selfloop = 1 if (n, n) in SG.edges else 0
                trace.append(Event({'time:timestamp': timestamps[n.split("=")[1]], 'concept:name': n.split("=")[0],
                                    'value': n.split("=")[1], 'typevalue': n, 'selfloop': selfloop}))
                if include_loops and selfloop:
                    trace.append(Event({'time:timestamp': timestamps[n.split("=")[1]], 'concept:name': n.split("=")[0],
                                        'value': n.split("=")[1], 'typevalue': n, 'selfloop': selfloop}))
            log.append(trace)
    log = sorting.sort_timestamp_log(log, "time:timestamp")
    return log
