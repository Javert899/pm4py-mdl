import pandas as pd
from frozendict import frozendict


class Shared:
    EKBE_ebeln_belnr = {}
    EKPO_ebeln_matnr = {}
    MSEG_mblnr_matnr = {}
    RSEG_belnr_matnr = {}
    EKKO_events = {}
    MKPF_events = {}
    RBKP_events = {}


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
        Shared.EKKO_events[el["EBELN"]].append({"event_activity": "ME21N", "event_timestamp": el["AEDAT"]})
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
                {"event_activity": el["TCODE"], "event_timestamp": el["event_timestamp"],
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
                {"event_activity": el["TCODE"], "event_timestamp": el["event_timestamp"],
                 "event_resource": el["USNAM"]})
            Shared.RBKP_events[el["BELNR"]] = sorted(Shared.RBKP_events[el["BELNR"]],
                                                     key=lambda x: x["event_timestamp"])


if __name__ == "__main__":
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
    print(Shared.RBKP_events)
