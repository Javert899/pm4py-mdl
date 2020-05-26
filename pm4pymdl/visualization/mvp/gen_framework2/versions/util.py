from collections import Counter


def get_events_edges_map(key, res):
    edges = [x for x in res["edges"] if x[0] == key]
    edges_map = {}
    for x in edges:
        k = (x[1], x[2])
        if k not in edges_map:
            edges_map[k] = Counter()
        edges_map[k][(x[3], x[4])] += res["edges"][x]
    for k in edges_map:
        edges_map[k] = len(edges_map[k])
    return edges_map


def get_objects_edges_map(key, res):
    edges = [x for x in res["edges"] if x[0] == key]
    edges_map = {}
    for x in edges:
        k = (x[1], x[2])
        if k not in edges_map:
            edges_map[k] = Counter()
        edges_map[k][x[5]] += res["edges"][x]
    for k in edges_map:
        edges_map[k] = len(edges_map[k])
    return edges_map


def get_eo_edges_map(key, res):
    edges = [x for x in res["edges"] if x[0] == key]
    edges_map = {}
    for x in edges:
        k = (x[1], x[2])
        if k not in edges_map:
            edges_map[k] = Counter()
        edges_map[k][(x[3], x[4])] += res["edges"][x]
    for k in edges_map:
        edges_map[k] = sum(edges_map[k].values())
    return edges_map


def get_edges_map(key, res, variant="events"):
    if variant == "events":
        return get_events_edges_map(key, res)
    elif variant == "eo":
        return get_eo_edges_map(key, res)
    elif variant == "objects":
        return get_objects_edges_map(key, res)

