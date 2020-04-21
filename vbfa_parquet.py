import pandas as pd
import os
import networkx
import datetime
from pm4py.objects.log.log import EventLog, Trace, Event
from pm4py.objects.conversion.log import factory as log_conv_factory
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter

G = networkx.DiGraph()
nodes = {}
timestamp = {}
dir = r"C:\Users\aless\Documents\sap_extraction"
path = os.path.join(dir, "VBFA.tsv")
df = pd.read_csv(path, sep="\t", dtype={"VBELN": str, "VBELV": str})
#df = df.sample(n=100)
df["event_timestamp"] = df["ERDAT"] + " " + df["ERZET"]
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
stream = df.to_dict("r")
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
        trace.append(Event({"concept:name": nodes[o], "time:timestamp": timestamp[o] if o in timestamp else datetime.datetime.fromtimestamp(1000000), "obj_id": o, "obj_parent": ""}))
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
                trace.append(Event({"concept:name": nodes[el], "time:timestamp": timestamp[el], "obj_id": el, "obj_parent": parents[el] if el in parents else ""}))
                for s in G.neighbors(el):
                    curr_nodes.append(s)
                    parents[s] = el
            i = i + 1
        """
        curr_nodes = list()
        for s in G.predecessors(o):
            curr_nodes.append(s)
        i = 0
        while i < len(curr_nodes):
            el = curr_nodes[i]
            if el not in visited_nodes:
                visited_nodes.append(el)
                trace.append({"concept:name": nodes[el], "time:timestamp": timestamp[el] if el in timestamp else datetime.datetime.fromtimestamp(1000000), "obj_id": el, "obj_parent": ""})
                for s in G.predecessors(el):
                    curr_nodes.append(s)
            i = i + 1
        """
    trace = sorted(trace, key=lambda x: x["time:timestamp"])
    trace1 = Trace(trace)
    trace1.attributes["concept:name"] = o
    log.append(trace1)
df = log_conv_factory.apply(log, variant=log_conv_factory.TO_DATAFRAME)
df = start_activities_filter.apply(df, [target_type])
activities = {}
activities["C"] = "Order"
activities["J"] = "Delivery"
activities["Q"] = "WMS Transfer Order"
activities["R"] = "Goods Movement"
activities["M"] = "Invoice"
activities["L"] = "Debit Memo Request"
activities["P"] = "Debit Memo"
activities["3"] = "Invoice List"
activities["U"] = "Pro Forma Invoice"
activities["H"] = "Returns"
df["concept:name"] = df["concept:name"].map(activities)
from pm4py.objects.log.exporter.parquet import factory as parquet_exporter
parquet_exporter.apply(df, "sales_document_flow.parquet")
