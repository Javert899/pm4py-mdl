from copy import deepcopy

def apply(stream):
    new_stream = []

    for ev in stream:
        keys = set(ev.keys())

        event_keys = [k for k in keys if k.startswith("event_")]
        object_keys = [k for k in keys if not k in event_keys]

        basic_event = {k: ev[k] for k in event_keys}

        for k in object_keys:
            if type(ev[k]) is str:
                if ev[k][0] == "[":
                    ev[k] = eval(ev[k])
                    #ev[k] = ev[k][1:-1].split(",")
            values = ev[k]
            if values is not None:
            #if values is not None and len(values) > 0:
                if not (str(values).lower() == "nan" or str(values).lower() == "nat"):
                    for v in values:
                        event = deepcopy(basic_event)
                        event[k] = v
                        new_stream.append(event)

    return new_stream
