import os
import pandas as pd
from frozendict import frozendict
from pm4pymdl.algo.mvp.gen_framework import factory as discovery
from pm4pymdl.visualization.mvp.gen_framework import factory as vis_factory

dir = r"C:\Users\aless\Documents\sap_extraction"


class Shared:
    activities = {}
    events = {}


def read_activities():
    tstct = pd.read_csv(os.path.join(dir, "TSTCT.tsv"), sep="\t")
    tstct = tstct[tstct["SPRSL"] == "E"]
    tstct = tstct[["TCODE", "TTEXT"]]
    stream = tstct.to_dict("r")
    for row in stream:
        Shared.activities[row["TCODE"]] = row["TTEXT"]


def extract_cdhdr():
    cdhdr = pd.read_csv(os.path.join(dir, "cdhdr.tsv"), sep="\t")
    cdhdr = cdhdr[["OBJECTCLAS", "OBJECTID", "USERNAME", "UDATE", "UTIME", "TCODE", "CHANGENR"]]
    cdpos = pd.read_csv(os.path.join(dir, "cdpos.tsv"), sep="\t")
    cdpos = cdpos[["CHANGENR", "VALUE_NEW"]]
    merged = pd.merge(cdhdr, cdpos, left_on="CHANGENR", right_on="CHANGENR", suffixes=["", "_2"])
    merged = merged.dropna(subset=["VALUE_NEW"])
    merged = merged[merged["VALUE_NEW"].isin(merged["OBJECTID"])]
    merged = pd.merge(merged, cdhdr, left_on="VALUE_NEW", right_on="OBJECTID", suffixes=["", "_3"])
    merged = merged.rename(columns={"USERNAME": "event_resource", "TCODE": "event_activity"})
    merged["event_timestamp"] = merged["UDATE"] + " " + merged["UTIME"]
    merged["event_timestamp"] = pd.to_datetime(merged["event_timestamp"])
    merged = merged.dropna(subset=["event_activity"])
    stream = merged.to_dict("r")
    for ev in stream:
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        Shared.events[key].add(frozendict({ev["OBJECTCLAS"]: ev["OBJECTID"]}))


def get_final_dataframe():
    stream = []
    keys = sorted(list(Shared.events.keys()), key=lambda x: x["event_timestamp"])
    for key in keys:
        for obj in Shared.events[key]:
            ev = dict(key)
            ev.update(dict(obj))
            stream.append(ev)
            act = ev["event_activity"]
            ev["event_activity"] = Shared.activities[act] if act in Shared.activities else act
            print(ev)
    dataframe = pd.DataFrame(stream)
    dataframe.type = "exploded"
    return dataframe


if __name__ == "__main__":
    read_activities()
    extract_cdhdr()
    dataframe = get_final_dataframe()
    print(dataframe)
    model = discovery.apply(dataframe, model_type_variant="model2", node_freq_variant="type21",
                            edge_freq_variant="type211")
    gviz = vis_factory.apply(model, parameters={"format": "svg"})
    vis_factory.view(gviz)
