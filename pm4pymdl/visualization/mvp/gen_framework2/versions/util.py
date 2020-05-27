from collections import Counter


def get_events_edges_map_performance(key, res):
    pass


def get_objects_edges_map_performance(key, res):
    pass


def get_eo_edges_map_performance(key, res):
    pass


def get_edges_map_performance(key, res, variant="events"):
    if variant == "events":
        return get_events_edges_map_performance(key, res)
    elif variant == "eo":
        return get_eo_edges_map_performance(key, res)
    elif variant == "objects":
        return get_objects_edges_map_performance(key, res)


def get_events_edges_map_frequency(key, res):
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


def get_objects_edges_map_frequency(key, res):
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


def get_eo_edges_map_frequency(key, res):
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


def get_edges_map_frequency(key, res, variant="events"):
    if variant == "events":
        return get_events_edges_map_frequency(key, res)
    elif variant == "eo":
        return get_eo_edges_map_frequency(key, res)
    elif variant == "objects":
        return get_objects_edges_map_frequency(key, res)


def get_edges_map(key, res, measure="frequency", variant="events"):
    if measure == "frequency":
        return get_edges_map_frequency(key, res, variant=variant)
    else:
        return get_edges_map_performance(key, res, variant=variant)


def get_activity_map_events_frequency(key, res):
    activities = [x for x in res["acti_spec"] if x[0] == key]
    activities_map = {}
    for x in activities:
        k = x[1]
        if k not in activities_map:
            activities_map[k] = Counter()
        activities_map[k][x[3]] += res["acti_spec"][x]
    for k in activities_map:
        activities_map[k] = len(activities_map[k])
    return activities_map


def get_activity_map_eo_frequency(key, res):
    activities = [x for x in res["acti_spec"] if x[0] == key]
    activities_map = {}
    for x in activities:
        k = x[1]
        if k not in activities_map:
            activities_map[k] = Counter()
        activities_map[k][x[3]] += res["acti_spec"][x]
    for k in activities_map:
        activities_map[k] = sum(activities_map[k])
    return activities_map


def get_activity_map_objects_frequency(key, res):
    activities = [x for x in res["acti_spec"] if x[0] == key]
    activities_map = {}
    for x in activities:
        k = x[1]
        if k not in activities_map:
            activities_map[k] = Counter()
        activities_map[k][x[4]] += res["acti_spec"][x]
    for k in activities_map:
        activities_map[k] = len(activities_map[k])
    return activities_map


def get_activity_map_frequency(key, res, variant="events"):
    if variant == "events":
        return get_activity_map_events_frequency(key, res)
    elif variant == "eo":
        return get_activity_map_eo_frequency(key, res)
    elif variant == "objects":
        return get_activity_map_objects_frequency(key, res)
