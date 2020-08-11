import networkx


def apply(df, source_attr, target_attr, type_attr):
    df[source_attr] = df[source_attr].astype(str)
    df[target_attr] = df[target_attr].astype(str)
    df[type_attr] = df[type_attr].astype(str)

    map_types = df[[source_attr, type_attr]].to_dict('r')
    map_types = {x[source_attr]: x[type_attr] for x in map_types if x[type_attr] != 'None'}

    map_source_target = dict(df.groupby([source_attr, target_attr]).size())
    map_source_target = list(map_source_target.keys())

    map_source_target = [(map_types[x]+"="+x, map_types[y]+"="+y) for (x, y) in map_source_target if x in map_types and y in map_types]
    map_types = {y+"="+x: y for x,y in map_types.items()}

    G = networkx.Graph()
    for k in map_types:
        G.add_node(k)
    for el in map_source_target:
        G.add_edge(el[0], el[1])

    return G

