from pm4pymdl.algo.mvp.utils import exploded_mdl_to_stream


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    stream = get_conf_stream_from_dataframe(df, parameters=parameters)

    return apply_stream(stream, model, parameters=parameters)


def get_conf_stream_from_dataframe(df, parameters=None):
    if parameters is None:
        parameters = {}

    df_type = df.type
    df = df.sort_values(["event_timestamp", "event_id"])
    if df_type == "exploded":
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

    return stream


def apply_stream(stream, model, parameters=None):
    if parameters is None:
        parameters = {}

    ret = []
    model = model.dictio

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
            if model["types_view"][t]["start_activities"][ak]["must"]:
                if t not in must_start:
                    must_start[t] = set()
                must_start[t].add(ak)
        for ak in model["types_view"][t]["end_activities"]:
            if model["types_view"][t]["end_activities"][ak]["must"]:
                if t not in must_end:
                    must_end[t] = set()
                must_end[t].add(ak)
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
                    if t in must_start and not ev["event_activity"] in must_start[t]:
                        ret[-1].add("start activity for object %s of type %s - should be %s instead is %s" % (
                            o, t, must_start[t], ev["event_activity"]))
                else:
                    prev = dictio_objs[t][o]
                    this_edge = (prev["event_activity"], ev["event_activity"])
                    rel_ev_obj = set(o2 for o2 in ev[t] if o2 in dictio_objs[t] and dictio_objs[t][o2] == prev)
                    if not (min_obj[t][this_edge] <= len(rel_ev_obj) <= max_obj[t][this_edge]):
                        ret[-1].add(
                            "number of related objects of type %s should be between %d and %d - instead is %d" % (
                                t, min_obj[t][this_edge], max_obj[t][this_edge], len(rel_ev_obj)))

                    if t in must_end and prev["event_activity"] in must_end[t]:
                        ret[-1].add(
                            "object %s of type %s - %s should be an end activity, instead is followed by %s!" % (
                                o, t, prev["event_activity"], ev["event_activity"]))
                    else:
                        if this_edge[1] in must_edges[t]:
                            if not must_edges[t][this_edge[1]] == this_edge[0]:
                                ret[-1].add(
                                    "object %s of type %s - %s should be preceded by %s, instead is preceded by %s!" % (
                                        o, t, this_edge[1], must_edges[t][this_edge[1]], this_edge[0]))
            for o in ev[t]:
                dictio_objs[t][o] = ev
    return ret
