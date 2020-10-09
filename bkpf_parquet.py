import pandas as pd
from pm4pymdl.util.parquet_exporter import exporter as parquet_exporter
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
import os

dir = r"C:\Users\aless\Documents\sap_extraction"
activities = {}
tstct = pd.read_csv(os.path.join(dir, "TSTCT.tsv"), sep="\t")
tstct = tstct[tstct["SPRSL"] == "E"]
tstct = tstct[["TCODE", "TTEXT"]]
stream = tstct.to_dict("r")
for row in stream:
    activities[row["TCODE"]] = row["TTEXT"]
bkpf = pd.read_csv(os.path.join(dir, "bkpf_old.tsv"), sep="\t", dtype={"BELNR": str, "AWKEY": str, "XBLNR": str, "BUKRS": str})
bkpf["time:timestamp"] = bkpf["CPUDT"] + " " + bkpf["CPUTM"]
bkpf["time:timestamp"] = pd.to_datetime(bkpf["time:timestamp"], format="%d.%m.%Y %H:%M:%S")
bkpf["case:concept:name"] = "C_" + bkpf["BELNR"]
bkpf["concept:name"] = bkpf["TCODE"].map(activities)
bkpf["org:resource"] = bkpf["USNAM"]
bkpf = bkpf.dropna(subset=["concept:name"])
bkpf = bkpf.dropna(subset=["org:resource"])
bkpf = bkpf.sort_values("time:timestamp")
bkpf = bkpf.reset_index()
bkpf = bkpf[[x for x in bkpf.columns if not "named:" in x]]
#print(start_activities_filter.get_start_activities(bkpf))
bkpf = start_activities_filter.apply(bkpf, ["Create Billing Document"])
#print(end_activities_filter.get_end_activities(bkpf))
bkpf = end_activities_filter.apply(bkpf, ["Post Document"])
parquet_exporter.apply(bkpf, "bkpf.parquet")
bkpf.to_csv("bkpf.csv", index=False)
