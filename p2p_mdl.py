import pandas as pd
from frozendict import frozendict


class Shared:
    EKBE_ebeln_belnr = {}
    EKPO_ebeln_matnr = {}
    MSEG_mblnr_matnr = {}
    RSEG_belnr_matnr = {}
    EKKO_events = {}


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
        Shared.EKKO_events[el["EBELN"]].append(frozendict({"event_activity": "ME21N", "event_timestamp": el["AEDAT"]}))
        Shared.EKKO_events[el["EBELN"]] = sorted(Shared.EKKO_events[el["EBELN"]], key=lambda x: x["event_timestamp"])


if __name__ == "__main__":
    read_ekbe()
    read_ekpo()
    read_mseg()
    read_rseg()
    read_ekko()
    #print(Shared.EKBE_ebeln_belnr)
    #print(Shared.EKPO_ebeln_matnr)
    #print(Shared.MSEG_mblnr_matnr)
    #print(Shared.RSEG_belnr_matnr)
    print(Shared.EKKO_events)
