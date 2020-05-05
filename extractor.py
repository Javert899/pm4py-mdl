import os
import pandas as pd
from frozendict import frozendict
from pm4pymdl.algo.mvp.gen_framework import factory as discovery
from pm4pymdl.visualization.mvp.gen_framework import factory as vis_factory
from pm4pymdl.algo.mvp.utils import clean_frequency, clean_arc_frequency

dir = r"C:\Users\aless\Documents\sap_extraction"


class Shared:
    ekbe = {}
    ekpo = {}
    vbfa = {}
    lips = {}
    vbak = {}
    activities = {}
    events = {}


def read_activities():
    tstct = pd.read_csv(os.path.join(dir, "TSTCT.tsv"), sep="\t")
    tstct = tstct[tstct["SPRSL"] == "E"]
    tstct = tstct[["TCODE", "TTEXT"]]
    stream = tstct.to_dict("r")
    for row in stream:
        Shared.activities[row["TCODE"]] = row["TTEXT"]


def remove_zeros(stru):
    i = 0
    while i < len(stru):
        if not stru[i] == "0":
            return stru[i:]
        i = i + 1
    return stru


def read_ekbe():
    ekbe = pd.read_csv(os.path.join(dir, "ekbe.tsv"), sep="\t", dtype={"BELNR": str, "EBELN": str})
    ekbe = ekbe[["BELNR", "EBELN"]]
    ekbe = ekbe.dropna(subset=["BELNR"])
    ekbe = ekbe.dropna(subset=["EBELN"])
    stream = ekbe.to_dict("r")
    for row in stream:
        if not row["BELNR"] in Shared.ekbe:
            Shared.ekbe[row["BELNR"]] = set()
        Shared.ekbe[row["BELNR"]].add(row["EBELN"])


def read_ekpo():
    ekpo = pd.read_csv(os.path.join(dir, "ekpo.tsv"), sep="\t", dtype={"EBELN": str, "BANFN": str})
    ekpo = ekpo[["BANFN", "EBELN"]]
    ekpo = ekpo.dropna(subset=["BANFN"])
    ekpo = ekpo.dropna(subset=["EBELN"])
    stream = ekpo.to_dict("r")
    for row in stream:
        if not row["EBELN"] in Shared.ekpo:
            Shared.ekpo[row["EBELN"]] = set()
        Shared.ekpo[row["EBELN"]].add(row["BANFN"])


def read_vbfa():
    vbfa = pd.read_csv(os.path.join(dir, "vbfa.tsv"), sep="\t", dtype={"VBELN": str, "VBELV": str})
    vbfa = vbfa[["VBELV", "VBELN"]]
    vbfa = vbfa.dropna(subset=["VBELV"])
    vbfa = vbfa.dropna(subset=["VBELN"])
    stream = vbfa.to_dict("r")
    for row in stream:
        if not row["VBELN"] in Shared.vbfa:
            Shared.vbfa[row["VBELN"]] = set()
        Shared.vbfa[row["VBELN"]].add(row["VBELV"])


def read_lips():
    lips = pd.read_csv(os.path.join(dir, "lips.tsv"), sep="\t", dtype={"VBELN": str, "VGBEL": str})
    lips = lips[["VBELN", "VGBEL"]]
    lips = lips.dropna(subset=["VBELN"])
    lips = lips.dropna(subset=["VGBEL"])
    stream = lips.to_dict("r")
    for row in stream:
        if not row["VBELN"] in Shared.lips:
            Shared.lips[row["VBELN"]] = set()
        Shared.lips[row["VBELN"]].add(row["VGBEL"])


def extract_cdhdr():
    cdhdr = pd.read_csv(os.path.join(dir, "cdhdr.tsv"), sep="\t", dtype={"OBJECTCLAS": str, "OBJECTID": str, "CHANGENR": str})
    cdhdr = cdhdr[["OBJECTCLAS", "OBJECTID", "USERNAME", "UDATE", "UTIME", "TCODE", "CHANGENR"]]
    cdpos = pd.read_csv(os.path.join(dir, "cdpos.tsv"), sep="\t", dtype={"CHANGENR": str, "VALUE_NEW": str})
    cdpos = cdpos[["CHANGENR", "VALUE_NEW"]]
    merged = pd.merge(cdhdr, cdpos, left_on="CHANGENR", right_on="CHANGENR", suffixes=["", "_2"])
    merged = merged.dropna(subset=["VALUE_NEW"])
    merged = merged[merged["VALUE_NEW"].isin(merged["OBJECTID"])]
    merged = pd.merge(merged, cdhdr, left_on="VALUE_NEW", right_on="OBJECTID", suffixes=["", "_3"])
    merged = merged.rename(columns={"USERNAME": "event_resource", "TCODE": "event_activity"})
    merged["event_timestamp"] = merged["UDATE"] + " " + merged["UTIME"]
    merged["event_timestamp"] = pd.to_datetime(merged["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
    merged = merged.dropna(subset=["event_activity"])
    merged = merged.dropna(subset=["event_resource"])
    stream = merged.to_dict("r")
    for ev in stream:
        #ev["OBJECTID"] = remove_zeros(ev["OBJECTID"])
        #ev["OBJECTID_3"] = remove_zeros(ev["OBJECTID_3"])
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        Shared.events[key].add(frozendict({ev["OBJECTCLAS"]: ev["OBJECTID"]}))
        Shared.events[key].add(frozendict({ev["OBJECTCLAS_3"]: ev["OBJECTID_3"]}))


def extract_rbkp():
    rbkp = pd.read_csv(os.path.join(dir, "rbkp.tsv"), sep="\t", dtype={"BELNR": str})
    rbkp = rbkp[["BELNR", "CPUDT", "CPUTM", "USNAM", "TCODE"]]
    rbkp = rbkp[rbkp["BELNR"].isin(Shared.ekbe.keys())]
    rbkp = rbkp.rename(columns={"USNAM": "event_resource", "TCODE": "event_activity"})
    rbkp["event_timestamp"] = rbkp["CPUDT"] + " " + rbkp["CPUTM"]
    rbkp["event_timestamp"] = pd.to_datetime(rbkp["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
    rbkp = rbkp.dropna(subset=["event_activity"])
    rbkp = rbkp.dropna(subset=["event_resource"])
    stream = rbkp.to_dict("r")
    for ev in stream:
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        Shared.events[key].add(frozendict({"BELNR": str(ev["BELNR"])}))


def extract_bkpf():
    bkpf = pd.read_csv(os.path.join(dir, "bkpf.tsv"), sep="\t", dtype={"BELNR": str, "AWKEY": str})
    bkpf = bkpf[["BELNR", "CPUDT", "CPUTM", "USNAM", "TCODE", "AWKEY"]]
    bkpf = bkpf[bkpf["BELNR"].isin(Shared.ekbe.keys()) | bkpf["AWKEY"].isin(Shared.vbak.keys())]
    bkpf = bkpf.rename(columns={"USNAM": "event_resource", "TCODE": "event_activity"})
    bkpf["event_timestamp"] = bkpf["CPUDT"] + " " + bkpf["CPUTM"]
    bkpf["event_timestamp"] = pd.to_datetime(bkpf["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
    bkpf = bkpf.dropna(subset=["event_activity"])
    bkpf = bkpf.dropna(subset=["event_resource"])
    stream = bkpf.to_dict("r")
    for ev in stream:
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        Shared.events[key].add(frozendict({"BELNR": str(ev["BELNR"])}))
        if ev["AWKEY"] in Shared.vbak:
            for el in Shared.vbak[ev["AWKEY"]]:
                Shared.events[key].add(el)


def extract_eban():
    eban = pd.read_csv(os.path.join(dir, "eban.tsv"), sep="\t", dtype={"BANFN": str})
    eban = eban[["BANFN", "ERDAT", "ERNAM"]]
    eban = eban.rename(columns={"ERNAM": "event_resource", "ERDAT": "event_timestamp"})
    eban["event_timestamp"] = pd.to_datetime(eban["event_timestamp"], format="%d.%m.%Y")
    eban["event_activity"] = "ME51N"
    eban = eban.dropna(subset=["event_activity"])
    eban = eban.dropna(subset=["event_resource"])
    stream = eban.to_dict("r")
    for ev in stream:
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        Shared.events[key].add(frozendict({"BANF": str(ev["BANFN"])}))


def extract_ekko():
    ekko = pd.read_csv(os.path.join(dir, "ekko.tsv"), sep="\t", dtype={"EBELN": str})
    ekko = ekko[["EBELN", "BEDAT", "ERNAM"]]
    ekko = ekko.rename(columns={"ERNAM": "event_resource", "BEDAT": "event_timestamp"})
    ekko["event_timestamp"] = pd.to_datetime(ekko["event_timestamp"], format="%d.%m.%Y")
    ekko["event_activity"] = "ME21N"
    ekko = ekko.dropna(subset=["event_activity"])
    ekko = ekko.dropna(subset=["event_resource"])
    stream = ekko.to_dict("r")
    for ev in stream:
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        ebeln = str(ev["EBELN"])
        Shared.events[key].add(frozendict({"EINKBELEG": ebeln}))

def get_final_dataframe():
    stream = []
    keys = sorted(list(Shared.events.keys()), key=lambda x: x["event_timestamp"])
    id = 0
    for index, key in enumerate(keys):
        id = id + 1
        for obj in Shared.events[key]:
            ev = dict(key)
            ev.update(dict(obj))
            ev["event_id"] = str(id)
            stream.append(ev)
            act = ev["event_activity"]
            ev["event_activity"] = Shared.activities[act] if act in Shared.activities else act
    dataframe = pd.DataFrame(stream)
    if "MATERIAL" in dataframe.columns:
        del dataframe["MATERIAL"]
    dataframe.type = "exploded"
    return dataframe


def extract_vbak():
    vbak = pd.read_csv(os.path.join(dir, "vbak.tsv"), sep="\t", dtype={"VBELN": str, "VBTYP": str})
    vbak = vbak[["VBELN", "ERDAT", "ERZET", "ERNAM", "VBTYP"]]
    vbak = vbak.rename(columns={"ERNAM": "event_resource"})
    vbak["event_timestamp"] = vbak["ERDAT"] + " " + vbak["ERZET"]
    vbak["event_timestamp"] = pd.to_datetime(vbak["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
    # ['C' 'I' 'B' 'H' 'K' 'G' 'A' 'E' 'F' 'W' 'D' 'L']
    # C => Create Sales Order VA01
    # I => Create Sales Order w/o charge VA01
    # B => Create Sales Quotation VA21
    # H => Create Sales Return FB75
    """
    A Inquiry
    B Quotation
    C Order
    D Item proposal
    E Scheduling agreement
    F Scheduling agreement with external service agent
    G Contract
    H Returns
    I Order w / o charge
    J Delivery
    K Credit memo request
    L Debit memo request
    M Invoice
    N Invoice cancellation
    O Credit memo
    P Debit memo
    Q WMS transfer order
    R Goods movement
    S Credit memo cancellation
    T Returns delivery for order
    U Pro forma invoice
    V Purchase order
    W Independent reqts plan
    X Handling unit
    """
    vbak = vbak.dropna(subset=["VBTYP"])
    vbak = vbak.dropna(subset=["event_resource"])
    stream = vbak.to_dict("r")
    for ev in stream:
        # K => VA01
        # G => VA41
        # A => VA11
        # E => VA31
        # F => VA31
        # W => MD61
        # D => VA51
        # L => VA01
        activity = None
        objtype = None
        if ev["VBTYP"] == "K":
            activity = "VA01"
            objtype = "VERKBELEG" # Create Sales Order
        elif ev["VBTYP"] == "G":
            activity = "VA41"
            objtype = "VERKBELEG" # Create Contract
        elif ev["VBTYP"] == "A":
            activity = "VA11"
            objtype = "VERKBELEG" # Create Inquiry
        elif ev["VBTYP"] == "E":
            activity = "VA31"
            objtype = "VERKBELEG" # Create Scheduling Agreement
        elif ev["VBTYP"] == "F":
            activity = "VA31"
            objtype = "VERKBELEG" # Create Scheduling Agreement
        elif ev["VBTYP"] == "W":
            activity = "MD61"
            objtype = "VERKBELEG" # Create Planned Indep. Requirements
        elif ev["VBTYP"] == "D":
            activity = "VA51"
            objtype = "VERKBELEG" # Create Item Proposal
        elif ev["VBTYP"] == "L":
            activity = "VA01"
            objtype = "VERKBELEG" # Create Sales Order
        if activity is not None:
            key = frozendict({"event_timestamp": ev["event_timestamp"],
                              "event_resource": ev["event_resource"], "event_activity": activity})
            if key not in Shared.events:
                Shared.events[key] = set()
            ebeln = str(ev["VBELN"])
            xx = frozendict({objtype: ebeln})
            Shared.events[key].add(xx)
            if ebeln not in Shared.vbak:
                Shared.vbak[ebeln] = set()
            Shared.vbak[ebeln].add(key)


def extract_likp():
    likp = pd.read_csv(os.path.join(dir, "likp.tsv"), sep="\t", dtype={"VBELN": str})
    likp = likp.rename(columns={"ERNAM": "event_resource"})
    likp["event_timestamp"] = likp["ERDAT"] + " " + likp["ERZET"]
    likp["event_timestamp"] = pd.to_datetime(likp["event_timestamp"], format="%d.%m.%Y %H:%M:%S")
    likp["event_activity"] = likp["TCODE"]
    likp = likp.dropna(subset=["event_activity"])
    likp = likp.dropna(subset=["event_resource"])
    stream = likp.to_dict("r")
    for ev in stream:
        key = frozendict({"event_timestamp": ev["event_timestamp"],
                          "event_resource": ev["event_resource"], "event_activity": ev["event_activity"]})
        if key not in Shared.events:
            Shared.events[key] = set()
        Shared.events[key].add(frozendict({"LIEFERUNG": ev["VBELN"]}))


def insert_ekpo_information():
    events = sorted(list(Shared.events.keys()), key=lambda x: x["event_timestamp"])
    ebeln_map = {}
    for index, eve in enumerate(events):
        eve_map = dict()
        for val in Shared.events[eve]:
            for k in val:
                eve_map[k] = val[k]
        if "EINKBELEG" in eve_map:
            val = eve_map["EINKBELEG"]
            if val in Shared.ekpo:
                ebeln_map[val] = eve
    for n in ebeln_map:
        eve = ebeln_map[n]
        corr = Shared.ekpo[n]
        for el in corr:
            Shared.events[eve].add(frozendict({"BANF": el}))


def insert_ekbe_information():
    events = sorted(list(Shared.events.keys()), key=lambda x: x["event_timestamp"])
    belnr_map = {}
    for index, eve in enumerate(events):
        eve_map = dict()
        for val in Shared.events[eve]:
            for k in val:
                eve_map[k] = val[k]
        if "BELNR" in eve_map:
            val = eve_map["BELNR"]
            if val in Shared.ekbe:
                belnr_map[val] = eve
    for n in belnr_map:
        eve = belnr_map[n]
        corr = Shared.ekbe[n]
        for el in corr:
            Shared.events[eve].add(frozendict({"EINKBELEG": el}))


def insert_vbfa_information():
    events = sorted(list(Shared.events.keys()), key=lambda x: x["event_timestamp"])
    vbeln_map = {}
    for index, eve in enumerate(events):
        eve_map = dict()
        for val in Shared.events[eve]:
            for k in val:
                eve_map[k] = val[k]
        if "VBELN" in eve_map:
            val = eve_map["VBELN"]
            if val in Shared.vbfa:
                vbeln_map[val] = eve
    for n in vbeln_map:
        eve = vbeln_map[n]
        corr = Shared.vbfa[n]
        for el in corr:
            Shared.events[eve].add(frozendict({"VERKBELEG": el}))


def insert_lips_information():
    events = sorted(list(Shared.events.keys()), key=lambda x: x["event_timestamp"])
    lips_map = {}
    for index, eve in enumerate(events):
        eve_map = dict()
        for val in Shared.events[eve]:
            for k in val:
                eve_map[k] = val[k]
        if "LIEFERUNG" in eve_map:
            val = eve_map["LIEFERUNG"]
            if val in Shared.lips:
                lips_map[val] = eve
    for n in lips_map:
        eve = lips_map[n]
        corr = Shared.lips[n]
        for el in corr:
            Shared.events[eve].add(frozendict({"VERKBELEG": el}))

if __name__ == "__main__":
    read_ekpo()
    read_ekbe()
    read_vbfa()
    read_lips()
    read_activities()
    extract_cdhdr()
    extract_vbak()
    extract_eban()
    extract_ekko()
    #extract_likp()
    #extract_rbkp()
    #extract_bkpf()
    insert_ekpo_information()
    insert_ekbe_information()
    insert_vbfa_information()
    insert_lips_information()
    if True:
        dataframe = get_final_dataframe()
        dataframe = clean_frequency.apply(dataframe, 4)
    if True:
        model = discovery.apply(dataframe, model_type_variant="model2", node_freq_variant="type21",
                                edge_freq_variant="type211")
        gviz = vis_factory.apply(model, parameters={"format": "svg", "min_edge_freq": 0})
        vis_factory.view(gviz)
