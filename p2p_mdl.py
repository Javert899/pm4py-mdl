import pandas as pd
from frozendict import frozendict
from copy import copy
import uuid

class Shared:
    TSTCT = {}
    EKBE_ebeln_belnr = {}
    EKPO_ebeln_matnr = {}
    MSEG_mblnr_matnr = {}
    RSEG_belnr_matnr = {}
    EKKO_events = {}
    MKPF_events = {}
    RBKP_events = {}
    events = []

def read_tstct():
    df = pd.read_csv("TSTCT.tsv", sep="\t", dtype={"SPRSL": str, "TCODE": str, "TTEXT": str})
    stream = df.to_dict('r')
    for el in stream:
        Shared.TSTCT[el["TCODE"]] = el["TTEXT"]


def get_activity(tcode):
    if tcode in Shared.TSTCT:
        return Shared.TSTCT[tcode]


def read_ekbe():
    df = pd.read_csv("EKBE.tsv", sep="\t", dtype={"EBELN": str, "EBELP": str, "BELNR": str})
    stream = df.to_dict('r')
    for el in stream:
        if str(el["BELNR"]).lower() != "nan":
            if not el["EBELN"] in Shared.EKBE_ebeln_belnr:
                Shared.EKBE_ebeln_belnr[el["EBELN"]] = set()
            Shared.EKBE_ebeln_belnr[el["EBELN"]].add(el["BELNR"])


def read_ekpo():
    df = pd.read_csv("EKPO.tsv", sep="\t", dtype={"EBELN": str, "EBELP": str, "MATNR": str, "BANFN": str, "BNFPO": str})
    stream = df.to_dict('r')
    for el in stream:
        if str(el["MATNR"]).lower() != "nan":
            if not el["EBELN"] in Shared.EKPO_ebeln_matnr:
                Shared.EKPO_ebeln_matnr[el["EBELN"]] = set()
            Shared.EKPO_ebeln_matnr[el["EBELN"]].add(el["MATNR"])


def read_mseg():
    df = pd.read_csv("MSEG.tsv", sep="\t", dtype={"MBLNR": str, "MATNR": str})
    stream = df.to_dict('r')
    for el in stream:
        if str(el["MATNR"]).lower() != "nan":
            if not el["MBLNR"] in Shared.MSEG_mblnr_matnr:
                Shared.MSEG_mblnr_matnr[el["MBLNR"]] = set()
            Shared.MSEG_mblnr_matnr[el["MBLNR"]].add(el["MATNR"])


def read_rseg():
    df = pd.read_csv("RSEG.tsv", sep="\t", dtype={"BELNR": str, "EBELN": str, "EBELP": str, "MATNR": str})
    stream = df.to_dict('r')
    for el in stream:
        if str(el["MATNR"]).lower() != "nan":
            if not el["BELNR"] in Shared.RSEG_belnr_matnr:
                Shared.RSEG_belnr_matnr[el["BELNR"]] = set()
            Shared.RSEG_belnr_matnr[el["BELNR"]].add(el["MATNR"])


def read_ekko():
    df = pd.read_csv("EKKO.tsv", sep="\t", dtype={"EBELN": str, "AEDAT": str, "ERNAM": str, "LIFNR": str})
    df["AEDAT"] = pd.to_datetime(df["AEDAT"], format="%d.%m.%Y", errors='coerce')
    stream = df.to_dict('r')
    for el in stream:
        if not el["EBELN"] in Shared.EKKO_events:
            Shared.EKKO_events[el["EBELN"]] = list()
        Shared.EKKO_events[el["EBELN"]].append({"event_activity": get_activity("ME21N"), "event_timestamp": el["AEDAT"]})
        Shared.EKKO_events[el["EBELN"]] = sorted(Shared.EKKO_events[el["EBELN"]], key=lambda x: x["event_timestamp"])


def read_mkpf():
    df = pd.read_csv("MKPF.tsv", sep="\t",
                     dtype={"MBLNR": str, "CPUDT": str, "CPUTM": str, "USNAM": str, "TCODE": str, "TCODE2": str})
    df["event_timestamp"] = df["CPUDT"] + " " + df["CPUTM"]
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    stream = df.to_dict('r')
    for el in stream:
        if str(el["TCODE"]).lower() != "nan":
            if not el["MBLNR"] in Shared.MKPF_events:
                Shared.MKPF_events[el["MBLNR"]] = list()
            Shared.MKPF_events[el["MBLNR"]].append(
                {"event_activity": get_activity(el["TCODE"]), "event_timestamp": el["event_timestamp"],
                 "event_resource": el["USNAM"]})
            Shared.MKPF_events[el["MBLNR"]] = sorted(Shared.MKPF_events[el["MBLNR"]],
                                                     key=lambda x: x["event_timestamp"])


def read_rbkp():
    df = pd.read_csv("RBKP.tsv", sep="\t", dtype={"BELNR": str, "USNAM": str, "TCODE": str, "CPUDT": str, "CPUTM": str})
    df["event_timestamp"] = df["CPUDT"] + " " + df["CPUTM"]
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    stream = df.to_dict('r')
    for el in stream:
        if str(el["TCODE"]).lower() != "nan":
            if not el["BELNR"] in Shared.RBKP_events:
                Shared.RBKP_events[el["BELNR"]] = list()
            Shared.RBKP_events[el["BELNR"]].append(
                {"event_activity": get_activity(el["TCODE"]), "event_timestamp": el["event_timestamp"],
                 "event_resource": el["USNAM"]})
            Shared.RBKP_events[el["BELNR"]] = sorted(Shared.RBKP_events[el["BELNR"]],
                                                     key=lambda x: x["event_timestamp"])


def write_events():
    for evk in Shared.EKKO_events:
        evs = Shared.EKKO_events[evk]
        i = 0
        while i < len(evs):
            ev = evs[i]
            ev["event_id"] = str(uuid.uuid4())
            nev = copy(ev)
            nev["EBELN"] = evk
            Shared.events.append(nev)
            if i == 0:
                if evk in Shared.EKPO_ebeln_matnr:
                    for mat in Shared.EKPO_ebeln_matnr[evk]:
                        nev = copy(ev)
                        nev["MATNR"] = mat
                        Shared.events.append(nev)
            i = i + 1


if __name__ == "__main__":
    read_tstct()
    read_ekbe()
    read_ekpo()
    read_mseg()
    read_rseg()
    read_ekko()
    read_mkpf()
    read_rbkp()
    # print(Shared.EKBE_ebeln_belnr)
    # print(Shared.EKPO_ebeln_matnr)
    # print(Shared.MSEG_mblnr_matnr)
    # print(Shared.RSEG_belnr_matnr)
    write_events()

