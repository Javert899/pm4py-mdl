from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.objects.conversion.log import converter
import math
from statistics import mean


def apply(df, parameters=None):
    if parameters is None:
        parameters = {}

    stream = get_stream_from_dataframe(df, parameters=parameters)

    return apply_stream(stream, parameters=parameters)


def get_stream_from_dataframe(df, parameters=None):
    if parameters is None:
        parameters = {}

    df_type = df.type
    df = df.sort_values(["event_timestamp", "event_id"])
    if df_type == "succint":
        df = succint_mdl_to_exploded_mdl.apply(df)

    columns = [x for x in df.columns if not x.startswith("event") or x == "event_activity" or x == "event_id" or x == "event_timestamp"]
    df = df[columns]

    stream = converter.apply(df, variant=converter.Variants.TO_EVENT_STREAM)

    return stream


def apply_stream(stream, parameters=None):
    if parameters is None:
        parameters = {}

    support = parameters["support"] if "support" in parameters else 1
    epsilon = parameters["epsilon"] if "epsilon" in parameters else 0.0
    debug = parameters["debug"] if "debug" in parameters else False
    noise_obj_number = parameters["noise_obj_number"] if "noise_obj_number" in parameters else 0.0

    types_lifecycle = {}

    eo = {}
    eot = {}
    eoe = {}
    ee = {}
    timestamps = {}

    for ev in stream:
        cl = [k for k in ev if not k.startswith("event_") and str(ev[k]) != "nan"][0]
        if not cl in types_lifecycle:
            types_lifecycle[cl] = {}
        o = ev[cl]
        if not o in types_lifecycle[cl]:
            types_lifecycle[cl][o] = []
        types_lifecycle[cl][o].append(ev)

    for t in types_lifecycle:
        eot[t] = dict()
        objects_lifecycle = types_lifecycle[t]
        for o in objects_lifecycle:
            evs = objects_lifecycle[o]
            i = 0
            while i < len(evs):
                i1 = evs[i]["event_id"]
                a1 = evs[i]["event_activity"]
                t1 = evs[i]["event_timestamp"].timestamp()
                if a1 not in eo:
                    eo[a1] = set()
                if a1 not in eot[t]:
                    eot[t][a1] = set()
                eo[a1].add((i1, o))
                eot[t][a1].add((i1, o))
                if i < len(evs)-1:
                    i2 = evs[i+1]["event_id"]
                    a2 = evs[i+1]["event_activity"]
                    t2 = evs[i+1]["event_timestamp"].timestamp()
                    if not (a1, t, a2) in eoe:
                        eoe[(a1, t, a2)] = set()
                        ee[(a1, t, a2)] = set()
                    eoe[(a1, t, a2)].add((i1, o, i2))
                    ee[(a1, t, a2)].add((i1, i2))
                    if not (i1, o, i2) in timestamps:
                        timestamps[(i1, o, i2)] = []
                    if not (i1, i2) in timestamps:
                        timestamps[(i1, i2)] = []
                    timestamps[(i1, o, i2)].append(t2-t1)
                    timestamps[(i1, i2)].append(t2-t1)
                i = i + 1

    for el in timestamps:
        timestamps[el] = mean(timestamps[el])

    ret = {}
    ret["activities"] = {}
    for act in eo:
        ret["activities"][act] = {}
        ret["activities"][act]["events"] = len({x[0] for x in eo[act]})
        ret["activities"][act]["objects"] = len({x[1] for x in eo[act]})
        ret["activities"][act]["eo"] = len(eo[act])
    ret["types_view"] = {}
    activities_mapping = {}
    activities_mapping_count = {}
    for t in types_lifecycle:
        ret["types_view"][t] = {"edges": {}, "activities": {}}
        for act in eot[t]:
            values = eot[t][act]
            ret["types_view"][t]["activities"][act] = {}
            ret["types_view"][t]["activities"][act]["events"] = {x[0] for x in values}
            ret["types_view"][t]["activities"][act]["objects"] = {x[1] for x in values}
            ret["types_view"][t]["activities"][act]["eo"] = values
        available_keys = {x for x in eoe.keys() if x[1] == t}
        for k in available_keys:
            a1 = k[0]
            a2 = k[2]
            values = eoe[k]
            values_ee = ee[k]
            values_timestamp_eoe = mean([timestamps[v] for v in values])
            values_timestamp_ee = mean([timestamps[v] for v in values_ee])
            g_1_2 = group_1_2(values)
            g_1_3 = group_1_3(values)
            g_1_2 = g_1_2[:math.ceil(len(g_1_2)*(1.0-noise_obj_number))]
            g_1_3 = g_1_3[:math.ceil(len(g_1_3)*(1.0-noise_obj_number))]
            g_1_2_min = min(g_1_2)
            g_1_2_max = max(g_1_2)
            g_1_3_min = min(g_1_3)
            g_1_3_max = max(g_1_3)
            ret["types_view"][t]["edges"][(a1, a2)] = {}
            ret["types_view"][t]["edges"][(a1, a2)]["events"] = {(x[0], x[2]) for x in values}
            ret["types_view"][t]["edges"][(a1, a2)]["objects"] = {x[1] for x in values}
            ret["types_view"][t]["edges"][(a1, a2)]["eo"] = values
            ret["types_view"][t]["edges"][(a1, a2)]["support_entry"] = ret["types_view"][t]["activities"][a2]["objects"].intersection(ret["types_view"][t]["edges"][(a1, a2)]["objects"])
            ret["types_view"][t]["edges"][(a1, a2)]["dev_entry"] = ret["types_view"][t]["activities"][a2]["objects"].difference(ret["types_view"][t]["edges"][(a1, a2)]["objects"])
            ret["types_view"][t]["edges"][(a1, a2)]["support_exit"] = ret["types_view"][t]["activities"][a1]["objects"].intersection(ret["types_view"][t]["edges"][(a1, a2)]["objects"])
            ret["types_view"][t]["edges"][(a1, a2)]["dev_exit"] = ret["types_view"][t]["activities"][a1]["objects"].difference(ret["types_view"][t]["edges"][(a1, a2)]["objects"])
            sen = len(ret["types_view"][t]["edges"][(a1, a2)]["support_entry"])
            den = len(ret["types_view"][t]["edges"][(a1, a2)]["dev_entry"])
            sex = len(ret["types_view"][t]["edges"][(a1, a2)]["support_exit"])
            dex = len(ret["types_view"][t]["edges"][(a1, a2)]["dev_exit"])
            if sen >= support and den/sen <= epsilon:
                ret["types_view"][t]["edges"][(a1, a2)]["must_entry"] = True
            else:
                ret["types_view"][t]["edges"][(a1, a2)]["must_entry"] = False
            if sex >= support and dex/sex <= epsilon:
                ret["types_view"][t]["edges"][(a1, a2)]["must_exit"] = True
            else:
                ret["types_view"][t]["edges"][(a1, a2)]["must_exit"] = False
            ret["types_view"][t]["edges"][(a1, a2)]["min_exit_obj"] = g_1_2_min
            ret["types_view"][t]["edges"][(a1, a2)]["max_exit_obj"] = g_1_2_max
            ret["types_view"][t]["edges"][(a1, a2)]["min_entry_obj"] = g_1_3_min
            ret["types_view"][t]["edges"][(a1, a2)]["max_entry_obj"] = g_1_3_max
            ret["types_view"][t]["edges"][(a1, a2)]["semantics"] = "EXI=%d..%d\nENT=%d..%d" % (g_1_2_min, g_1_2_max, g_1_3_min, g_1_3_max)
            ret["types_view"][t]["edges"][(a1, a2)]["performance_events"] = values_timestamp_ee
            ret["types_view"][t]["edges"][(a1, a2)]["performance_eo"] = values_timestamp_eoe
        for edge in ret["types_view"][t]["edges"]:
            ret["types_view"][t]["edges"][edge]["events"] = len(ret["types_view"][t]["edges"][edge]["events"])
            ret["types_view"][t]["edges"][edge]["objects"] = len(ret["types_view"][t]["edges"][edge]["objects"])
            ret["types_view"][t]["edges"][edge]["eo"] = len(ret["types_view"][t]["edges"][edge]["eo"])
        for act in ret["types_view"][t]["activities"]:
            o = len(ret["types_view"][t]["activities"][act]["objects"])
            if act not in activities_mapping or activities_mapping_count[act] < o:
                activities_mapping[act] = t
                activities_mapping_count[act] = o
            ret["types_view"][t]["activities"][act]["events"] = len(ret["types_view"][t]["activities"][act]["events"])
            ret["types_view"][t]["activities"][act]["objects"] = o
            ret["types_view"][t]["activities"][act]["eo"] = len(ret["types_view"][t]["activities"][act]["eo"])
        ret["activities_mapping"] = activities_mapping

    return ret


def group_1_2(values):
    ret = {}
    for val in values:
        e1 = val[0]
        o = val[1]
        if not e1 in ret:
            ret[e1] = set()
        ret[e1].add(o)
    return list(len(x) for x in ret.values())


def group_1_3(values):
    ret = {}
    for val in values:
        e1 = val[2]
        o = val[1]
        if not e1 in ret:
            ret[e1] = set()
        ret[e1].add(o)
    return list(len(x) for x in ret.values())
