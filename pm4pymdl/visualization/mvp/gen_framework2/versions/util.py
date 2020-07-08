from collections import Counter
from statistics import mean


def projection(edges_map, reference_map, type="no", return_acti_map=False):
    acti_assignation_map = {}
    acti_assignation_score = {}
    for k in reference_map:
        for a in reference_map[k]:
            if not a in acti_assignation_map or reference_map[k][a] > acti_assignation_score[a]:
                acti_assignation_map[a] = k
                acti_assignation_score[a] = reference_map[k][a]

    if not type == "no":
        for k in edges_map:
            if type == "source":
                edges_map[k] = {x: y for x, y in edges_map[k].items() if acti_assignation_map[x[0]] == k}
            elif type == "target":
                edges_map[k] = {x: y for x, y in edges_map[k].items() if acti_assignation_map[x[1]] == k}

    if return_acti_map:
        return edges_map, acti_assignation_map
    return edges_map


def projection_edges_freq(edges_map, events_map, min_edge_freq):
    for k in events_map:
        for k2 in events_map[k]:
            if events_map[k][k2] < min_edge_freq:
                del edges_map[k][k2]

    return edges_map


def human_readable_stat(c):
    """
    Transform a timedelta expressed in seconds into a human readable string

    Parameters
    ----------
    c
        Timedelta expressed in seconds

    Returns
    ----------
    string
        Human readable string
    """
    c = int(float(c))
    years = c // 31104000
    months = c // 2592000
    days = c // 86400
    hours = c // 3600 % 24
    minutes = c // 60 % 60
    seconds = c % 60
    if years > 0:
        return str(years) + "Y"
    if months > 0:
        return str(months) + "MO"
    if days > 0:
        return str(days) + "D"
    if hours > 0:
        return str(hours) + "h"
    if minutes > 0:
        return str(minutes) + "m"
    return str(seconds) + "s"


def get_events_edges_map_performance(key, res):
    edges = [x for x in res["edges"] if x[0] == key]
    edges_map = {}
    for x in edges:
        k = (x[1], x[2])
        if k not in edges_map:
            edges_map[k] = dict()
        edges_map[k][(x[3], x[4])] = x[7].timestamp() - x[6].timestamp()
    for k in edges_map:
        edges_map[k] = mean(edges_map[k].values())
    return edges_map


def get_objects_edges_map_performance(key, res):
    edges = [x for x in res["edges"] if x[0] == key]
    edges_map = {}
    for x in edges:
        k = (x[1], x[2])
        if k not in edges_map:
            edges_map[k] = dict()
        edges_map[k][x[5]] = x[7].timestamp() - x[6].timestamp()
    for k in edges_map:
        edges_map[k] = mean(edges_map[k].values())
    return edges_map


def get_eo_edges_map_performance(key, res):
    edges = [x for x in res["edges"] if x[0] == key]
    edges_map = {}
    for x in edges:
        k = (x[1], x[2])
        if k not in edges_map:
            edges_map[k] = list()
        edges_map[k].append(x[7].timestamp() - x[6].timestamp())
    for k in edges_map:
        edges_map[k] = mean(edges_map[k])
    return edges_map


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
        activities_map[k][x[2]] += res["acti_spec"][x]
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
        activities_map[k][x[2]] += res["acti_spec"][x]
    for k in activities_map:
        activities_map[k] = sum(activities_map[k].values())
    return activities_map


def get_activity_map_objects_frequency(key, res):
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


def get_activity_map_frequency(key, res, variant="events"):
    if variant == "events":
        return get_activity_map_events_frequency(key, res)
    elif variant == "eo":
        return get_activity_map_eo_frequency(key, res)
    elif variant == "objects":
        return get_activity_map_objects_frequency(key, res)
