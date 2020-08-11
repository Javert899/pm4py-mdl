import networkx
from pm4py.objects.log.log import EventLog, Trace, Event
from datetime import datetime


def apply(df, source_attr, target_attr, type_attr, timestamp_key):
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
        G.add_edge(el[0], el[1])

    conn_comp = sorted(list(networkx.connected_components(networkx.Graph(G))), key=lambda x: len(x), reverse=True)

    return G, conn_comp, timestamps


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
                trace.append(Event({'time:timestamp': timestamps[n.split("=")[1]], 'concept:name': n.split("=")[0],
                                    'value': n.split("=")[1], 'typevalue': n}))
                """if (n,n) in SG.edges:
                    timest = timest + 1
                    trace.append(Event(
                        {'time:timestamp': datetime.fromtimestamp(timest), 'concept:name': n.split("=")[0],
                         'value': n.split("=")[1], 'typevalue': n}))"""
            log.append(trace)
    return log
