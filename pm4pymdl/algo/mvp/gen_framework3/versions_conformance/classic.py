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

    must_edges = {}
    dictio_objs = {}

    for t in model["types_view"]:
        dictio_objs[t] = {}
        must_edges[t] = {}
        for ek in model["types_view"][t]["edges"]:
            e = model["types_view"][t]["edges"][ek]
            if e["must"]:
                must_edges[t][ek[1]] = e

    for i in range(len(stream)):
        ev = stream[i]
        class_keys = [k for k in ev.keys() if not k.startswith("event_")]
        for c in class_keys:
            pass

    return ret
