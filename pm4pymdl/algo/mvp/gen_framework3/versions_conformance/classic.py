from pm4pymdl.algo.mvp.utils import exploded_mdl_to_stream


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    ret = []
    model = model.dictio

    if df.type == "exploded":
        stream = exploded_mdl_to_stream.apply(df)
    else:
        stream = df.to_dict('r')

    for ev in stream:
        keys = list(ev.keys())
        for k in keys:
            if str(ev[k]) == "nan":
                del ev[k]
            elif not k.startswith("event_") and type(ev[k]) == str and ev[k][0] == "[":
                ev[k] = eval(ev[k])

    must_start = {}
    must_end = {}
    must_edges = {}
    dictio_objs = {}
    min_obj = {}
    max_obj = {}

    for t in model["types_view"]:
        dictio_objs[t] = {}
        must_edges[t] = {}
        min_obj[t] = {}
        max_obj[t] = {}
        for ak in model["types_view"][t]["start_activities"]:
            must_start[t] = ak
        for ak in model["types_view"][t]["end_activities"]:
            must_end[t] = ak
        for ek in model["types_view"][t]["edges"]:
            e = model["types_view"][t]["edges"][ek]
            if e["must"]:
                must_edges[t][ek[1]] = ek[0]
            min_obj[t][ek] = e["min_obj"]
            max_obj[t][ek] = e["max_obj"]

    for i in range(len(stream)):
        ret.append(set())
        ev = stream[i]
        class_keys = [k for k in ev.keys() if not k.startswith("event_")]
        for t in class_keys:
            for o in ev[t]:
                if not o in dictio_objs[t]:
                    if t in must_start and not ev["event_activity"] == must_start[t]:
                        ret[-1].add("start activity for object %s of type %s - should be %s instead is %s" % (
                            o, t, must_start[t], ev["event_activity"]))
                else:
                    prev = dictio_objs[t][o]
                    this_edge = (prev["event_activity"], ev["event_activity"])
                    if not (min_obj[t][this_edge] <= len(ev[t]) <= max_obj[t][this_edge]):
                        ret[-1].add("number of related objects should be between %d and %d - instead is %d" % (
                            min_obj[t][this_edge], max_obj[t][this_edge], len(ev[t])))

                    if t in must_end and prev["event_activity"] == must_end[t]:
                        ret[-1].add("object %s of type %s - %s should be end activity!" % (o, t, must_end[t]))
                    else:
                        if this_edge[1] in must_edges[t]:
                            if not must_edges[t][this_edge[1]] == this_edge[0]:
                                ret[-1].add(
                                    "object %s of type %s - %s should be preceded by %s, instead is preceded by %s!" % (
                                    o, t, this_edge[1], must_edges[t][this_edge[1]], this_edge[0]))

                dictio_objs[t][o] = ev
    return ret