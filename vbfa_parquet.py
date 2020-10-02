import pandas as pd
import os
import networkx
import datetime
from pm4py.objects.log.log import EventLog, Trace, Event
from pm4py.objects.conversion.log import converter as log_conv_factory
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
from frozendict import frozendict

class Shared:
    vbeln = {}
    timestamp_column = "time:timestamp"
    activity_column = "concept:name"
    dir = r"C:\Users\aless\Documents\sap_extraction"
    tcodes = {}
    associated_events = {}
    enable_filling_events = False

def extract_cdhdr():
    cdhdr = pd.read_csv(os.path.join(Shared.dir, "cdhdr.tsv"), sep="\t",
                        dtype={"OBJECTCLAS": str, "OBJECTID": str, "CHANGENR": str})
    cdhdr = cdhdr[["OBJECTCLAS", "OBJECTID", "USERNAME", "UDATE", "UTIME", "TCODE", "CHANGENR"]]
    cdpos = pd.read_csv(os.path.join(Shared.dir, "cdpos.tsv"), sep="\t", dtype={"CHANGENR": str, "VALUE_NEW": str})
    cdpos = cdpos[["CHANGENR", "VALUE_NEW"]]
    merged = pd.merge(cdhdr, cdpos, left_on="CHANGENR", right_on="CHANGENR", suffixes=["", "_2"])
    merged = merged.dropna(subset=["VALUE_NEW"])
    allowed_keys = set()
    for k in Shared.vbeln:
        allowed_keys.add(k)
    merged = merged[merged["VALUE_NEW"].isin(merged["OBJECTID"]) | merged["VALUE_NEW"].isin(allowed_keys)]
    merged = pd.merge(merged, cdhdr, left_on="VALUE_NEW", right_on="OBJECTID", suffixes=["", "_3"])
    merged = merged.rename(columns={"USERNAME": "event_resource", "TCODE": Shared.activity_column})
    merged[Shared.timestamp_column] = merged["UDATE"] + " " + merged["UTIME"]
    merged[Shared.timestamp_column] = pd.to_datetime(merged[Shared.timestamp_column], format="%d.%m.%Y %H:%M:%S")
    merged = merged.dropna(subset=[Shared.activity_column])
    merged = merged.dropna(subset=["event_resource"])
    stream = merged.to_dict("r")
    for ev in stream:
        if ev["OBJECTID"] not in Shared.associated_events:
            Shared.associated_events[ev["OBJECTID"]] = []
        Shared.associated_events[ev["OBJECTID"]].append(
            frozendict({Shared.activity_column: ev[Shared.activity_column], Shared.timestamp_column: ev[Shared.timestamp_column],
             "obj_id": ev["VALUE_NEW"], "obj_parent": ev["OBJECTID"], "obj_type": "", "TCODE": ev[Shared.activity_column]}))
    for o in Shared.associated_events:
        Shared.associated_events[o] = frozenset(Shared.associated_events[o])


def fill_event(e):
    if Shared.enable_filling_events:
        if e["obj_type"] == "C":
            if e["obj_id"] in Shared.vbeln:
                e.update(Shared.vbeln[e["obj_id"]])


def insert_missing_events(trace):
    obj_ids = set(x["obj_id"] for x in trace)
    for o in obj_ids:
        if o in Shared.associated_events:
            for ev in Shared.associated_events[o]:
                trace.append(Event(dict(ev)))
    return trace

def read_activities():
    tstct = pd.read_csv(os.path.join(Shared.dir, "TSTCT.tsv"), sep="\t")
    tstct = tstct[tstct["SPRSL"] == "E"]
    tstct = tstct[["TCODE", "TTEXT"]]
    stream = tstct.to_dict("r")
    for row in stream:
        Shared.tcodes[row["TCODE"]] = row["TTEXT"]


def read_vbak():
    vbak = pd.read_csv(os.path.join(Shared.dir, "vbak.tsv"), sep="\t", dtype={"VBELN": str, "VBTYP": str})
    vbak[Shared.timestamp_column] = vbak["ERDAT"] + " " + vbak["ERZET"]
    vbak[Shared.timestamp_column] = pd.to_datetime(vbak[Shared.timestamp_column], format="%d.%m.%Y %H:%M:%S")
    vbak = vbak.to_dict("r")
    Shared.vbeln = {ev["VBELN"]: ev for ev in vbak}


if __name__ == "__main__":
    read_activities()
    read_vbak()
    extract_cdhdr()
    G = networkx.DiGraph()
    nodes = {}
    timestamp = {}
    path = os.path.join(Shared.dir, "VBFA.tsv")
    vbfa = pd.read_csv(path, sep="\t", dtype={"VBELN": str, "VBELV": str})
    # df = df.sample(n=100)
    vbfa["event_timestamp"] = vbfa["ERDAT"] + " " + vbfa["ERZET"]
    vbfa["event_timestamp"] = pd.to_datetime(vbfa["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
    stream = vbfa.to_dict("r")
    for ev in stream:
        id1 = ev["VBELV"]
        id2 = ev["VBELN"]
        typ1 = ev["VBTYP_V"]
        typ2 = ev["VBTYP_N"]
        if not id1 in G.nodes:
            nodes[id1] = typ1
            G.add_node(id1)
        if not id2 in G.nodes:
            nodes[id2] = typ2
            G.add_node(id2)
        G.add_edge(id1, id2)
        timestamp[id2] = ev["event_timestamp"]
    target_type = "C"
    orders = [n for n in nodes if nodes[n] == target_type]
    log = EventLog()
    for o in orders:
        parents = {}
        if o in timestamp:
            trace = []
            e = {Shared.activity_column: nodes[o],
                 Shared.timestamp_column: timestamp[o] if o in timestamp else datetime.datetime.fromtimestamp(1000000),
                 "obj_id": o, "obj_parent": "", "obj_type": nodes[o]}
            fill_event(e)
            trace.append(Event(e))
            visited_nodes = list()
            curr_nodes = list()
            for s in G.neighbors(o):
                curr_nodes.append(s)
                parents[s] = o
            i = 0
            while i < len(curr_nodes):
                el = curr_nodes[i]
                if el not in visited_nodes and nodes[el] != target_type:
                    visited_nodes.append(el)
                    e = {Shared.activity_column: nodes[el], Shared.timestamp_column: timestamp[el], "obj_id": el,
                         "obj_parent": parents[el] if el in parents else "", "obj_type": nodes[el]}
                    fill_event(e)
                    trace.append(Event(e))
                    for s in G.neighbors(el):
                        curr_nodes.append(s)
                        parents[s] = el
                i = i + 1
            trace = insert_missing_events(trace)
            trace = sorted(trace, key=lambda x: x[Shared.timestamp_column])
            trace1 = Trace(trace)
            trace1.attributes["concept:name"] = o
            log.append(trace1)
    df = log_conv_factory.apply(log, variant=log_conv_factory.TO_DATAFRAME)
    df = start_activities_filter.apply(df, [target_type])
    unique_values = set(df[Shared.activity_column].unique())
    activities = {x: x for x in unique_values}
    activities["C"] = "Create Order"
    activities["J"] = "Create Delivery"
    activities["Q"] = "WMS Transfer Order"
    activities["R"] = "Goods Movement"
    activities["M"] = "Create Invoice"
    activities["L"] = "Create Debit Memo Request"
    activities["P"] = "Create Debit Memo"
    activities["U"] = "Create Pro Forma Invoice"
    activities["H"] = "Create Returns Document"
    activities.update(Shared.tcodes)
    df[Shared.activity_column] = df[Shared.activity_column].map(activities)
    df = df.dropna(subset=[Shared.activity_column])
    df = df[[x for x in df.columns if "named:" not in x]]
    df.type = "exploded"
    df.to_csv("sales_document_flow.csv", index=False)
    #from pm4py.objects.log.exporter.parquet import factory as parquet_exporter

    #parquet_exporter.apply(df, "sales_document_flow.parquet")
