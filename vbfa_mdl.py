import pandas as pd
import os
import networkx
import datetime
from frozendict import frozendict
from copy import deepcopy

class Shared:
    vbeln = {}
    timestamp_column = "event_timestamp"
    activity_column = "event_activity"
    dir = r"C:\Users\aless\Documents\sap_extraction"
    tcodes = {}
    associated_events = {}
    enable_filling_events = False
    unmapped = set()

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


def get_class_from_type(typ):
    dct = {}
    dct["C"] = "VERKBELEG"
    dct["J"] = "HANDL_UNIT"
    dct["Q"] = "WMS_TRANSFER"
    dct["R"] = "CHARGE"
    dct["M"] = "BELNR"
    dct["L"] = "DEBIT_MEMO_REQ"
    dct["P"] = "DEBIT_MEMO_DOC"
    dct["U"] = "INVOICE_PRO_FORMA"
    dct["H"] = "RETURNS_DOC"

    dct["A"] = "VERKBELEG" # inquiry
    dct["T"] = "RETURNS_DELIVERY"
    dct["D"] = "ITEM_PROPOSAL"
    dct["V"] = "INFOSATZ" # purchase order
    dct["N"] = "INVOICE_CANCEL"
    dct["E"] = "EINKBELEG" # scheduling agreement
    dct["O"] = "CREDIT_MEMO_DOC"
    dct["K"] = "CREDIT_MEMO_REQ"
    dct["B"] = "VERKBELEG" # quotation
    dct["G"] = "VERKBELEG" # contract
    dct["W"] = "INDIP_REQ" # indipendent requisition
    dct["I"] = "ORD_WO_CHARGE" # order without charge
    dct["X"] = "HANDL_UNIT" # handling unit
    if typ in dct:
        return dct[typ]
    Shared.unmapped.add(typ)
    return "C_" + str(typ)


if __name__ == "__main__":
    read_activities()
    read_vbak()
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
    events = set()
    id = 0
    for node in G.nodes:
        id = id + 1
        typ = nodes[node]
        cl = get_class_from_type(typ)
        timest = timestamp[node] if node in timestamp else datetime.datetime.fromtimestamp(1000000)
        basic_event = {Shared.activity_column: typ, Shared.timestamp_column: timest, "event_id": str(id)}
        event = deepcopy(basic_event)
        event[cl] = node
        events.add(frozendict(event))
        for other_node in G.predecessors(node):
            event = deepcopy(basic_event)
            otyp = nodes[other_node]
            ocl = get_class_from_type(otyp)
            event[ocl] = other_node
            events.add(frozendict(event))
    events = [dict(x) for x in events]
    df = pd.DataFrame(events)
    df.type = "exploded"
    #unique_values = set(df[Shared.activity_column].unique())
    #activities = {x: x for x in unique_values}
    activities = {}
    activities["C"] = "Create Order"
    activities["J"] = "Create Delivery"
    activities["Q"] = "WMS Transfer Order"
    activities["R"] = "Goods Movement"
    activities["M"] = "Create Invoice"
    activities["L"] = "Create Debit Memo Request"
    activities["P"] = "Create Debit Memo"
    activities["U"] = "Create Pro Forma Invoice"
    activities["H"] = "Create Returns Document"
    """
    dct["A"] = "VERKBELEG" # inquiry
    dct["T"] = "RETURNS_DELIVERY"
    dct["D"] = "ITEM_PROPOSAL"
    dct["V"] = "INFOSATZ" # purchase order
    dct["N"] = "INVOICE_CANCEL"
    dct["E"] = "EINKBELEG" # scheduling agreement
    dct["O"] = "CREDIT_MEMO_DOC"
    dct["K"] = "CREDIT_MEMO_REQ"
    dct["B"] = "VERKBELEG" # quotation
    dct["G"] = "VERKBELEG" # contract
    dct["W"] = "INDIP_REQ" # indipendent requisition
    dct["I"] = "ORD_WO_CHARGE" # order without charge
    dct["X"] = "HANDL_UNIT" # handling unit
    """
    activities["A"] = "Create Inquiry"
    activities["T"] = "Returns Delivery"
    activities["D"] = "Item Proposal"
    activities["V"] = "Create Purchase Order"
    activities["N"] = "Invoice Cancellation"
    activities["E"] = "Scheduling Agreement"
    activities["O"] = "Create Credit Memo"
    activities["K"] = "Create Credit Memo Request"
    activities["B"] = "Create Quotation"
    activities["G"] = "Create Contract"
    activities["W"] = "Indipendent Requisition"
    activities["I"] = "Create Order without Charge"
    activities["X"] = "Handling Unit"
    activities.update(Shared.tcodes)
    df[Shared.activity_column] = df[Shared.activity_column].map(activities)
    df = df.dropna(subset=[Shared.activity_column])
    print(df)
    df = df[[x for x in df.columns if "named:" not in x]]
    df = df.sort_values(Shared.timestamp_column)
    df.to_csv("sap.mdl", index=False)
