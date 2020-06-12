import pandas as pd
import os
import networkx
import datetime
from frozendict import frozendict
from copy import deepcopy
import uuid


class Shared:
    vbeln = {}
    timestamp_column = "event_timestamp"
    activity_column = "event_activity"
    dir = r"../sap_extraction"
    tcodes = {}
    associated_events = {}
    enable_filling_events = False
    unmapped = set()
    timestamps = {}
    event_map = {}
    bkpf = {}
    vbap = {}
    ekpo = {}
    lips = {}


def read_vbap():
    vbap = pd.read_csv(os.path.join(Shared.dir, "vbap.tsv"), sep="\t", dtype={"VBELN": str, "MATNR": str})
    vbap = vbap.dropna(subset=["VBELN"])
    vbap = vbap.dropna(subset=["MATNR"])
    vbap = vbap.to_dict("r")
    for el in vbap:
        vbeln = el["VBELN"]
        matnr = el["MATNR"]
        if not vbeln in Shared.vbap:
            Shared.vbap[vbeln] = set()
        Shared.vbap[vbeln].add(matnr)


def read_ekpo():
    ekpo = pd.read_csv(os.path.join(Shared.dir, "ekpo.tsv"), sep="\t", dtype={"EBELN": str, "MATNR": str})
    ekpo = ekpo.dropna(subset=["EBELN"])
    ekpo = ekpo.dropna(subset=["MATNR"])
    ekpo = ekpo.to_dict("r")
    for ev in ekpo:
        ebeln = ev["EBELN"]
        matnr = ev["MATNR"]
        if ebeln not in Shared.ekpo:
            Shared.ekpo[ebeln] = set()
        Shared.ekpo[ebeln].add(matnr)


def read_lips():
    lips = pd.read_csv(os.path.join(Shared.dir, "lips.tsv"), sep="\t", dtype={"VBELN": str, "MATNR": str})
    lips = lips.dropna(subset=["VBELN"])
    lips = lips.dropna(subset=["MATNR"])
    lips = lips.to_dict("r")
    for ev in lips:
        vbeln = ev["VBELN"]
        matnr = ev["MATNR"]
        if vbeln not in Shared.lips:
            Shared.lips[vbeln] = set()
        Shared.lips[vbeln].add(matnr)


def read_ekko():
    ekko = pd.read_csv(os.path.join(Shared.dir, "ekko.tsv"), sep="\t", dtype={"EBELN": str})
    ekko = ekko[["EBELN", "BEDAT", "ERNAM"]]
    ekko = ekko.rename(columns={"ERNAM": "event_resource", "BEDAT": Shared.timestamp_column})
    ekko[Shared.timestamp_column] = pd.to_datetime(ekko[Shared.timestamp_column], format="%d.%m.%Y")
    ekko[Shared.activity_column] = "ME21N"
    ekko = ekko.dropna(subset=[Shared.activity_column])
    stream = ekko.to_dict("r")
    events = set()
    for ev in stream:
        ebeln = ev["EBELN"]
        if ebeln in Shared.ekpo:
            basic_event = {Shared.timestamp_column: ev[Shared.timestamp_column],
                           Shared.activity_column: ev[Shared.activity_column], "event_id": str(uuid.uuid4()),
                           "event_objid": ebeln, "event_objtype": "EINKBELEG"}
            event = deepcopy(basic_event)
            event["EINKBELEG"] = ebeln
            events.add(frozendict(event))
            for el in Shared.ekpo[ebeln]:
                event = deepcopy(basic_event)
                event["MATERIAL"] = el
                events.add(frozendict(event))
    return events


def read_eban():
    eban = pd.read_csv(os.path.join(Shared.dir, "eban.tsv"), sep="\t", dtype={"BANFN": str, "MATNR": str})
    eban["ERDAT"] = pd.to_datetime(eban["ERDAT"], format="%d.%m.%Y")
    eban = eban.rename(columns={"ERDAT": Shared.timestamp_column})
    eban = eban.to_dict("r")
    # EINKBELEG
    events = set()
    for ev in eban:
        basic_event = {Shared.timestamp_column: ev[Shared.timestamp_column], Shared.activity_column: "ME51N",
                       "event_id": str(uuid.uuid4()), "event_objid": ev["BANFN"], "event_objtype": "EINKBELEG"}
        ev0 = deepcopy(basic_event)
        ev0["EINKBELEG"] = ev["BANFN"]
        ev1 = deepcopy(basic_event)
        ev1["MATERIAL"] = ev["MATNR"]
        events.add(frozendict(ev0))
        events.add(frozendict(ev1))
    return events


def extract_bkpf():
    bkpf = pd.read_csv(os.path.join(Shared.dir, "bkpf.tsv"), sep="\t", dtype={"BELNR": str, "AWKEY": str})
    bkpf = bkpf[["BELNR", "CPUDT", "CPUTM", "USNAM", "TCODE", "AWKEY"]]
    bkpf = bkpf.rename(columns={"USNAM": "event_resource", "TCODE": Shared.activity_column})
    bkpf[Shared.timestamp_column] = bkpf["CPUDT"] + " " + bkpf["CPUTM"]
    bkpf[Shared.timestamp_column] = pd.to_datetime(bkpf[Shared.timestamp_column], format="%d.%m.%Y %H:%M:%S")
    stream = bkpf.to_dict("r")
    for ev in stream:
        awkey = ev["AWKEY"]
        if awkey not in Shared.bkpf:
            Shared.bkpf[awkey] = set()
        Shared.bkpf[awkey].add(frozendict(
            {Shared.timestamp_column: ev[Shared.timestamp_column], Shared.activity_column: ev[Shared.activity_column],
             "event_id": str(uuid.uuid4()), "event_objtype": "BELNR", "event_objid": ev["BELNR"]}))


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
    # vbak = vbak[["VBELN", Shared.timestamp_column]]
    vbak = vbak.to_dict("r")
    Shared.vbeln = {ev["VBELN"]: frozendict({"event_" + x: y for x, y in ev.items()}) for ev in vbak}
    Shared.timestamps = {ev["VBELN"]: ev[Shared.timestamp_column] for ev in vbak}
    # print(Shared.vbeln)


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

    dct["A"] = "VERKBELEG"  # inquiry
    dct["T"] = "RETURNS_DELIVERY"
    dct["D"] = "ITEM_PROPOSAL"
    dct["V"] = "INFOSATZ"  # purchase order
    dct["N"] = "INVOICE_CANCEL"
    dct["E"] = "EINKBELEG"  # scheduling agreement
    dct["O"] = "CREDIT_MEMO_DOC"
    dct["K"] = "CREDIT_MEMO_REQ"
    dct["B"] = "VERKBELEG"  # quotation
    dct["G"] = "VERKBELEG"  # contract
    dct["W"] = "INDIP_REQ"  # indipendent requisition
    dct["I"] = "ORD_WO_CHARGE"  # order without charge
    dct["X"] = "HANDL_UNIT"  # handling unit
    if typ in dct:
        return dct[typ]
    Shared.unmapped.add(typ)
    return "C_" + str(typ)


if __name__ == "__main__":
    read_lips()
    read_ekpo()
    ekko_events = read_ekko()
    read_vbap()
    eban_events = read_eban()
    print(eban_events)
    #eban_events = set()
    extract_bkpf()
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
        timest = timestamp[node] if node in timestamp else (
            Shared.timestamps[node] if node in Shared.timestamps else None)
        if timest is not None:
            basic_event = {Shared.activity_column: typ, Shared.timestamp_column: timest, "event_id": str(id),
                           "event_objtype": cl, "event_objid": node}
            event = deepcopy(basic_event)
            event[cl] = node
            Shared.event_map[node] = cl
            events.add(frozendict(event))
            for other_node in G.predecessors(node):
                event = deepcopy(basic_event)
                otyp = nodes[other_node]
                ocl = get_class_from_type(otyp)
                event[ocl] = other_node
                events.add(frozendict(event))
            if node in Shared.vbap:
                for matnr in Shared.vbap[node]:
                    event = deepcopy(basic_event)
                    event["MATERIAL"] = matnr
                    events.add(frozendict(event))
            if node in Shared.lips:
                for matnr in Shared.lips[node]:
                    event = deepcopy(basic_event)
                    event["MATERIAL"] = matnr
                    events.add(frozendict(event))
    events_to_add = set()
    for awkey in Shared.bkpf:
        for ev in Shared.bkpf[awkey]:
            print(ev)
            events_to_add.add(ev)
    """for ev in events:
        if ev["event_objid"] in Shared.bkpf:
            for nev0 in Shared.bkpf[ev["event_objid"]]:
                nev1 = dict(nev0)
                nev1["BELNR"] = nev1["event_objid"]
                events_to_add.add(frozendict(nev1))
                nev2 = dict(nev0)
                nev2[ev["event_objtype"]] = ev["event_objid"]
                events_to_add.add(frozendict(nev2))"""
    for ev in events_to_add:
        events.add(ev)
    for ev in eban_events:
        events.add(ev)
    for ev in ekko_events:
        events.add(ev)
    events = [dict(x) for x in events]
    """for ev in events:
        if ev["event_objid"] in Shared.vbeln:
            ev.update(Shared.vbeln[ev["event_objid"]])"""
    df = pd.DataFrame(events)
    df.type = "exploded"
    # unique_values = set(df[Shared.activity_column].unique())
    # activities = {x: x for x in unique_values}
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
    df = df[[x for x in df.columns if "named:" not in x]]
    allowed_columns = [x for x in df.columns if not x.startswith("C_") and not x.startswith("event_")]
    df = df.dropna(subset=allowed_columns, how="all")
    df = df.sort_values(Shared.timestamp_column)
    print(df)
    df.type = "exploded"
    from pm4pymdl.objects.mdl.exporter import factory as mdl_exporter

    mdl_exporter.apply(df, "sap.mdl")
