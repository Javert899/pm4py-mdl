import pandas as pd
from pm4pymdl.algo.mvp.gen_framework2 import general
from pm4pymdl.algo.mvp.utils import clean_frequency, clean_arc_frequency
from pm4py.objects.conversion.log import factory as log_conv_factory
from pm4py.objects.log.log import EventStream
from pm4py.algo.discovery.alpha import factory as alpha_miner
from collections import Counter


def apply(df0, classifier_function=None, parameters=None):
    if parameters is None:
        parameters = {}

    if classifier_function is None:
        classifier_function = lambda x: x["event_activity"]

    min_acti_freq = parameters["min_acti_freq"] if "min_acti_freq" in parameters else 0
    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 0

    df = df0.copy()
    df = general.preprocess(df, parameters=parameters)

    df = clean_frequency.apply(df, min_acti_freq=min_acti_freq)
    df = clean_arc_frequency.apply(df, min_freq=min_edge_freq)

    models = {}

    obj_types = [x for x in df.columns if not x.startswith("event_")]
    activities_repeated = Counter()
    activities = set()

    for ot in obj_types:
        new_df = df[["event_id", "event_activity", "event_timestamp", ot]].dropna(subset=[ot])
        new_df = new_df.rename(
            columns={ot: "case:concept:name", "event_timestamp": "time:timestamp"})
        log = new_df.to_dict("r")
        for ev in log:
            ev["event_objtype"] = ot
            ev["concept:name"] = classifier_function(ev)
            del ev["event_objtype"]
            del ev["event_activity"]
            activities.add((ev["event_id"], ev["concept:name"]))

        log = EventStream(log)
        this_activities = set(x["concept:name"] for x in log)
        for act in this_activities:
            activities_repeated[act] += 1
        log = log_conv_factory.apply(log, variant=log_conv_factory.TO_EVENT_LOG)

        models[ot] = alpha_miner.apply(log, parameters=parameters)

    activities_repeated = set(x for x in activities_repeated if activities_repeated[x] > 1)
    activities = dict(Counter(list(x[1] for x in activities)))

    return {"type": "petri", "models": models, "activities": activities, "activities_repeated": activities_repeated}
