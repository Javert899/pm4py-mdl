from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.objects.conversion.log import converter


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

    types_lifecycle = {}

    eo = {}
    eot = {}
    eoe = {}
    ee = {}

    for ev in stream:
        cl = [k for k in ev if not k.startswith("event_") and str(ev[k]) != "nan"][0]
        if not cl in types_lifecycle:
            types_lifecycle[cl] = {}
        o = ev[cl]
        if not o in types_lifecycle[cl]:
            types_lifecycle[cl][o] = []
        types_lifecycle[cl][o].append(ev)

    for t in types_lifecycle:
        objects_lifecycle = types_lifecycle[t]
        for o in objects_lifecycle:
            evs = objects_lifecycle[o]
            i = 0
            while i < len(evs):
                i1 = evs[i]["event_id"]
                a1 = evs[i]["event_activity"]
                if a1 not in eo:
                    eo[a1] = set()
                    eot[a1] = {}
                if t not in eot[a1]:
                    eot[a1][t] = set()
                eo[a1].add((i1, o))
                if i < len(evs)-1:
                    i2 = evs[i+1]["event_id"]
                    a2 = evs[i+1]["event_activity"]
                    if not (a1, t, a2) in eoe:
                        eoe[(a1, t, a2)] = set()
                        ee[(a1, t, a2)] = set()
                    eoe[(a1, t, a2)].add((i1, o, i2))
                    ee[(a1, t, a2)].add((i1, i2))
                i = i + 1

    ret = {}
    ret["activities"] = {}
    for act in eo:
        ret["activities"][act] = {}
        ret["activities"][act]["events"] = len({x[0] for x in eo[act]})
        ret["activities"][act]["objects"] = len({x[1] for x in eo[act]})
        ret["activities"][act]["eo"] = len(eo[act])
    ret["types_view"] = {}
    for t in types_lifecycle:
        ret["types_view"][t] = {"edges": {}}
        available_keys = {x for x in eoe.keys() if x[1] == t}
        for k in available_keys:
            a1 = k[0]
            a2 = k[2]
            values = eoe[k]
            ret["types_view"][t][(a1, a2)] = {}
            ret["types_view"][t][(a1, a2)]["events"] = len({x[0] for x in values})
            ret["types_view"][t][(a1, a2)]["objects"] = len({x[1] for x in values})
            ret["types_view"][t][(a1, a2)]["eo"] = len(values)

    return ret
