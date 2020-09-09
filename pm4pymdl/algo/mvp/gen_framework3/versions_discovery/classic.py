from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4pymdl.algo.mvp.gen_framework3 import model
from pm4py.objects.conversion.log import converter
from collections import Counter
import sys


def apply(df, parameters=None):
    if parameters is None:
        parameters = {}

    support = parameters["support"] if "support" in parameters else 1
    epsilon = parameters["epsilon"] if "epsilon" in parameters else 0
    debug = parameters["debug"] if "debug" in parameters else False

    ret = {}

    df_type = df.type
    df = df.sort_values(["event_timestamp", "event_id"])
    if df_type == "succint":
        df = succint_mdl_to_exploded_mdl.apply(df)

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
                    activities_local[act] = {"objects": set(), "eo": set(), "preceded_by": dict(),
                                             "followed_by": dict()}
                activities[act]["events"].add(ev["event_id"])
                activities[act]["objects"].add(ev[t])
                activities[act]["eo"].add((ev["event_id"], ev[t]))
                activities_local[act]["objects"].add(ev[t])
                activities_local[act]["eo"].add((ev["event_id"], ev[t]))
                if i == 0:
                    if act not in start_activities:
                        start_activities[act] = {"events": set(), "objects": set(), "eo": set(), "must": False,
                                                 "min_obj": 1, "max_obj": sys.maxsize}
                    start_activities[act]["events"].add(ev["event_id"])
                    start_activities[act]["objects"].add(ev[t])
                    start_activities[act]["eo"].add((ev["event_id"], ev[t]))
                if i > 0:
                    if not evs[i - 1]["event_activity"] in activities_local[act]["preceded_by"]:
                        activities_local[act]["preceded_by"][evs[i - 1]["event_activity"]] = set()
                    activities_local[act]["preceded_by"][evs[i - 1]["event_activity"]].add((ev["event_id"], ev[t]))
                if i < len(evs) - 1:
                    if not evs[i + 1]["event_activity"] in activities_local[act]["followed_by"]:
                        activities_local[act]["followed_by"][evs[i + 1]["event_activity"]] = set()
                    activities_local[act]["followed_by"][evs[i + 1]["event_activity"]].add((ev["event_id"], ev[t]))
                if i == len(evs) - 1:
                    if act not in end_activities:
                        end_activities[act] = {"events": set(), "objects": set(), "eo": set(), "must": False,
                                               "min_obj": 1, "max_obj": sys.maxsize}
                    end_activities[act]["events"].add(ev["event_id"])
                    end_activities[act]["objects"].add(ev[t])
                    end_activities[act]["eo"].add((ev["event_id"], ev[t]))
            for i in range(len(evs) - 1):
                ev0 = evs[i]
                ev1 = evs[i + 1]
                if (ev0["event_activity"], ev1["event_activity"]) not in edges:
                    edges[((ev0["event_activity"], ev1["event_activity"]))] = {"events": set(), "objects": set(),
                                                                               "eo": set(), "eeo": set(),
                                                                               "min_obj": 1, "max_obj": sys.maxsize,
                                                                               "must": False}
                edges[(ev0["event_activity"], ev1["event_activity"])]["events"].add(ev1["event_id"])
                edges[(ev0["event_activity"], ev1["event_activity"])]["objects"].add(ev1[t])
                edges[(ev0["event_activity"], ev1["event_activity"])]["eo"].add((ev1["event_id"], ev1[t]))
                edges[(ev0["event_activity"], ev1["event_activity"])]["eeo"].add(
                    (ev0["event_id"] + "@@@" + ev1["event_id"], ev1[t]))

        for edge in edges:
            eo_dict = {}
            for eo in edges[edge]["eeo"]:
                eo_dict[eo[0]] = set()
            for eo in edges[edge]["eeo"]:
                eo_dict[eo[0]].add(eo[1])
            for e in eo_dict:
                eo_dict[e] = len(eo_dict[e])
            min_obj = min(eo_dict[x] for x in eo_dict)
            max_obj = max(eo_dict[x] for x in eo_dict)

            if not edge[0] == edge[1]:
                predecessors = sorted(list(activities_local[edge[1]]["preceded_by"]), reverse=True,
                                      key=lambda x: len(activities_local[edge[1]]["preceded_by"][x]))
                num = activities_local[edge[1]]["preceded_by"][predecessors[0]]
                den = activities_local[edge[1]]["preceded_by"][predecessors[0]]

                for i in range(1, len(predecessors)):
                    den = den.union(activities_local[edge[1]]["preceded_by"][predecessors[i]])

                q0 = 1 - len(num) / len(den) if len(den) > 0 else sys.maxsize

                if q0 <= epsilon:
                    set_difference = edges[edge]["eo"].difference(
                        activities_local[edge[1]]["preceded_by"][predecessors[0]])
                    set_intersection = edges[edge]["eo"].intersection(
                        activities_local[edge[1]]["preceded_by"][predecessors[0]])

                    q1 = len(set_intersection)
                    q2 = len(set_difference) / len(set_intersection) if len(set_intersection) > 0 else sys.maxsize

                    if debug:
                        print("edge ", edge, "q1=", q1, "q2=", q2)
                    if q1 >= support and q2 <= epsilon:
                        edges[edge]["must"] = True

            edges[edge]["min_obj"] = min_obj
            edges[edge]["max_obj"] = max_obj
            edges[edge]["events"] = len(edges[edge]["events"])
            edges[edge]["objects"] = len(edges[edge]["objects"])
            edges[edge]["eo"] = len(edges[edge]["eo"])
            edges[edge]["eeo"] = len(edges[edge]["eeo"])

        for act in start_activities:
            eo_dict = {}
            for eo in start_activities[act]["eo"]:
                eo_dict[eo[0]] = set()
            for eo in start_activities[act]["eo"]:
                eo_dict[eo[0]].add(eo[1])
            for e in eo_dict:
                eo_dict[e] = len(eo_dict[e])
            min_obj = min(eo_dict[x] for x in eo_dict)
            max_obj = max(eo_dict[x] for x in eo_dict)

            start_activities[act]["min_obj"] = min_obj
            start_activities[act]["max_obj"] = max_obj

            set_intersection = start_activities[act]["eo"].intersection(activities_local[act]["eo"])
            set_difference = start_activities[act]["eo"].difference(activities_local[act]["eo"])

            q1 = len(set_intersection)
            q2 = len(set_difference) / len(set_intersection) if len(set_intersection) > 0 else sys.maxsize

            if debug:
                print("start_activity ", act, "q1=", q1, "q2=", q2)
            if q1 >= support and q2 <= epsilon:
                start_activities[act]["must"] = True

            start_activities[act]["events"] = len(start_activities[act]["events"])
            start_activities[act]["objects"] = len(start_activities[act]["objects"])
            start_activities[act]["eo"] = len(start_activities[act]["eo"])

        for act in end_activities:
            eo_dict = {}
            for eo in end_activities[act]["eo"]:
                eo_dict[eo[0]] = set()
            for eo in end_activities[act]["eo"]:
                eo_dict[eo[0]].add(eo[1])
            for e in eo_dict:
                eo_dict[e] = len(eo_dict[e])
            min_obj = min(eo_dict[x] for x in eo_dict)
            max_obj = max(eo_dict[x] for x in eo_dict)

            end_activities[act]["min_obj"] = min_obj
            end_activities[act]["max_obj"] = max_obj

            set_intersection = activities_local[act]["eo"].intersection(end_activities[act]["eo"])
            set_difference = activities_local[act]["eo"].difference(end_activities[act]["eo"])

            q1 = len(set_intersection)
            q2 = len(set_difference) / len(set_intersection) if len(set_intersection) > 0 else sys.maxsize

            if debug:
                print("end_activity ", act, "q1=", q1, "q2=", q2)
            if q1 >= support and q2 <= epsilon:
                end_activities[act]["must"] = True

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
