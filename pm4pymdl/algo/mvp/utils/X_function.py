from frozendict import frozendict


def create_index(succint_dataframe):
    events = succint_dataframe.to_dict("r")
    index = {}
    for idx, ev in enumerate(events):
        keys = [x for x in ev.keys() if not x.startswith("event_")]
        for k in keys:
            if not k in index:
                index[k] = {}
            events[idx][k] = tuple(eval(events[idx][k]))
        # events[idx] = frozendict(events[idx])

    for ev in events:
        keys = [x for x in ev.keys() if not x.startswith("event_")]
        for k in keys:
            for val in ev[k]:
                if not val in index[k]:
                    index[k][val] = set()
                index[k][val].add(ev["event_id"])

    events_dict = {x["event_id"]: x for x in events}

    return events_dict, index


def apply(events_dict, index, event, provided_keys=None):
    curr = [event["event_id"]]
    i = 0
    while i < len(curr):
        ev = events_dict[curr[i]]
        this_timestamp = ev["event_timestamp"]
        keys = set(x for x in ev.keys() if not x.startswith("event_"))
        if provided_keys is not None:
            keys = keys.intersection(provided_keys)
        for k in keys:
            for val in ev[k]:
                for evv in index[k][val]:
                    if not evv in curr:
                        if events_dict[evv]["event_timestamp"] > this_timestamp:
                            curr.append(evv)
        i = i + 1
    return set(curr)


def filter_dataframe(dataframe, events_dict, index, event, provided_keys=None):
    X_result = apply(events_dict, index, event, provided_keys=provided_keys)
    return dataframe[dataframe["event_id"].isin(X_result)]


def signature(events_dict, index, event, provided_keys=None):
    X_result = apply(events_dict, index, event, provided_keys=provided_keys)
    sign = dict()

    for evk in X_result:
        ev = events_dict[evk]
        keys = set(x for x in ev.keys() if not x.startswith("event_"))
        if provided_keys is not None:
            keys = keys.intersection(provided_keys)
        for k in keys:
            if not k in sign or sign[k] < len(ev[k]):
                sign[k] = len(ev[k])

    sign = tuple(sorted([(x, y) for x, y in sign.items()]))
    return sign


def equivalence_classes(events_dict, index, activity_to_consider=None, provided_keys=None):
    classes = {}
    for evk in events_dict:
        ev = events_dict[evk]
        if activity_to_consider is None or ev["event_activity"] == activity_to_consider:
            sign = signature(events_dict, index, ev, provided_keys=provided_keys)
            if not sign in classes:
                classes[sign] = set()
            classes[sign].add(evk)
    return classes
