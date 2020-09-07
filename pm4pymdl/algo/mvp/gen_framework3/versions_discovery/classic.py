from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4pymdl.algo.mvp.gen_framework3 import model
from pm4py.objects.conversion.log import converter
from collections import Counter
import sys


def apply(df, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}

    df = succint_mdl_to_exploded_mdl.apply(df)

    df = df.sort_values(["event_timestamp", "event_id"])
    columns = [x for x in df.columns if not x.startswith("event") or x == "event_activity" or x == "event_id"]
    df = df[columns]

    stream = converter.apply(df, variant=converter.Variants.TO_EVENT_STREAM)

    types_lifecycle = {}
    types_view = {}

    for ev in stream:
        cl = [k for k in ev if not k.startswith("event_") and str(ev[k]) != "nan"][0]
        if not cl in types_lifecycle:
            types_lifecycle[cl] = {}
        o = ev[cl]
        if not o in types_lifecycle[cl]:
            types_lifecycle[cl][o] = []
        types_lifecycle[cl][o].append(ev)

    activities = {}

    for t in types_lifecycle:
        objects_lifecycle = types_lifecycle[t]

        activities_local = {}
        start_activities = dict()
        end_activities = dict()

        edges = {}
        for o in objects_lifecycle:
            evs = objects_lifecycle[o]
            for i in range(len(evs)):
                ev = evs[i]
                act = ev["event_activity"]
                if act not in activities:
                    activities[act] = {"events": set(), "objects": set(), "eo": set()}
                if act not in activities_local:
                    activities_local[act] = {"objects": set(), "preceded_by": Counter(), "followed_by": Counter()}
                activities[act]["events"].add(ev["event_id"])
                activities[act]["objects"].add(ev[t])
                activities[act]["eo"].add((ev["event_id"], ev[t]))
                activities_local[act]["objects"].add(ev[t])
                if i == 0:
                    if act not in start_activities:
                        start_activities[act] = {"events": set(), "objects": set(), "eo": set()}
                    start_activities[act]["events"].add(ev["event_id"])
                    start_activities[act]["objects"].add(ev[t])
                    start_activities[act]["eo"].add((ev["event_id"], ev[t]))
                if i > 0:
                    activities_local[act]["preceded_by"][evs[i - 1]["event_activity"]] += 1
                if i < len(evs) - 1:
                    activities_local[act]["followed_by"][evs[i - 1]["event_activity"]] += 1
                if i == len(evs) - 1:
                    if act not in end_activities:
                        end_activities[act] = {"events": set(), "objects": set(), "eo": set()}
                    end_activities[act]["events"].add(ev["event_id"])
                    end_activities[act]["objects"].add(ev[t])
                    end_activities[act]["eo"].add((ev["event_id"], ev[t]))
            for i in range(len(evs) - 1):
                ev0 = evs[i]
                ev1 = evs[i + 1]
                if (ev0["event_activity"], ev1["event_activity"]) not in edges:
                    edges[((ev0["event_activity"], ev1["event_activity"]))] = {"events": set(), "objects": set(),
                                                                               "eo": set(),
                                                                               "min_obj": 1, "max_obj": sys.maxsize,
                                                                               "must": False}

        for edge in edges:
            edges[edge]["events"] = len(edges[edge]["events"])
            edges[edge]["objects"] = len(edges[edge]["objects"])
            edges[edge]["eo"] = len(edges[edge]["eo"])

        for act in start_activities:
            start_activities[act]["events"] = len(start_activities[act]["events"])
            start_activities[act]["objects"] = len(start_activities[act]["objects"])
            start_activities[act]["eo"] = len(start_activities[act]["eo"])

        for act in end_activities:
            end_activities[act]["events"] = len(end_activities[act]["events"])
            end_activities[act]["objects"] = len(end_activities[act]["objects"])
            end_activities[act]["eo"] = len(end_activities[act]["eo"])

        types_view[t] = {"start_activities": start_activities, "end_activities": end_activities,
                         "activities_local": activities_local, "edges": edges}

    for act in activities:
        activities[act]["events"] = len(activities[act]["events"])
        activities[act]["objects"] = len(activities[act]["objects"])
        activities[act]["eo"] = len(activities[act]["eo"])

    ret["activities"] = activities
    ret["types_view"] = types_view

    return model.ObjCentricMultigraph(ret)

