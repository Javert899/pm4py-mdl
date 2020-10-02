import pandas as pd
from pm4pymdl.algo.mvp.gen_framework2 import general
from pm4pymdl.algo.mvp.utils import clean_frequency, clean_arc_frequency
from pm4py.objects.conversion.log import converter as log_conv_factory
from pm4py.objects.log.log import EventStream
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from collections import Counter
from pm4py.objects.log.util import sorting

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
    #df = clean_arc_frequency.apply(df, min_freq=min_edge_freq)

    models = {}

    obj_types = [x for x in df.columns if not x.startswith("event_")]
    activities = set()
    activities_repeated = Counter()
    edges = Counter()
    start_activities = dict()
    end_activities = dict()
    acti_spec = Counter()

    for ot in obj_types:
        start_activities[ot] = set()
        end_activities[ot] = set()

        new_df = df[["event_id", "event_activity", "event_timestamp", ot]].dropna(subset=[ot])
        new_df = new_df.sort_values("event_timestamp")
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
        log = sorting.sort_timestamp(log, "time:timestamp")

        for trace in log:
            if trace:
                start_activities[ot].add(trace[0]["concept:name"])
                end_activities[ot].add(trace[-1]["concept:name"])
                for i in range(len(trace) - 1):
                    ev0 = trace[i]
                    ev1 = trace[i + 1]
                    edges[(ot, ev0["concept:name"], ev1["concept:name"], ev0["event_id"], ev1["event_id"], trace.attributes["concept:name"], ev0["time:timestamp"], ev1["time:timestamp"])] += 1
                    acti_spec[(ot, trace[i]["concept:name"], trace[i]["event_id"], trace.attributes["concept:name"], trace[i]["time:timestamp"])] += 1
                if len(trace) > 0:
                    acti_spec[(ot, trace[-1]["concept:name"], trace[-1]["event_id"], trace.attributes["concept:name"], trace[-1]["time:timestamp"])] += 1

        models[ot] = set(y["concept:name"] for x in log for y in x)

    activities = dict(Counter(list(x[1] for x in activities)))
    activities_repeated = set(x for x in activities_repeated if activities_repeated[x] > 1)

    return {"type": "dfg", "models": models, "activities": activities, "activities_repeated": activities_repeated,
            "edges": edges, "start_activities": start_activities, "end_activities": end_activities, "acti_spec": acti_spec}
