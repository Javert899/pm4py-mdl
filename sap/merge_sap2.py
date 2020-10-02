import pandas as pd
from dateutil.parser import parse
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl

cdhdr = pd.read_csv("cdhdr.csv", sep=",", quotechar="\"")
cdpos = pd.read_csv("cdpos.csv", sep=",", quotechar="\"")

cols_hdr = [x for x in cdhdr.columns if not x.startswith("Unnamed")]
cols_pos = [x for x in cdpos.columns if not x.startswith("Unnamed")]

cdhdr = cdhdr[cols_hdr]
cdpos = cdpos[cols_pos]

print(cdhdr)
print(cdpos)

merged = cdhdr.merge(cdpos, how="left", left_on=["changenr", "objectid"], right_on=["changenr", "objectid"])
merged["@@index"] = merged.index

stream = merged.to_dict("r")

new_stream = []

for event in stream:
    new_event = None
    new_event = {}
    if not (str(event["tcode"]) == "nan" or  str(event["tabname"]) == "nan"):
        new_event["event_id"] = event["changenr"]
        new_event["event_activity"] = event["tcode"]
        new_event["event_tcode"] = event["tcode"]
        new_event["event_timestamp"] = parse(event["udate"])
        new_event["resource"] = event["username"]

        new_event["obj@@" + event["objectclas_x"]] = event["objectid"]
        new_event["table@@" + str(event["tabname"])] = str(event["tabkey"])+":="+str(event["value_new"])

        new_stream.append(new_event)

dataframe = pd.DataFrame(new_stream)
dataframe.type = "exploded"
succint_table = exploded_mdl_to_succint_mdl.apply(dataframe)
col_mapping = {x:x for x in succint_table.columns if not x.startswith("table")}
col_mapping2 = {x:"event_"+x for x in succint_table.columns if x.startswith("table")}
col_mapping.update(col_mapping2)

#for col in col_mapping2.values():
#    print(col)
#    succint_table[col] = succint_table[col].astype(str)

def f(x):
    if str(x).lower() != 'nan':
        return sorted(list(set(y for y in x if not "TRIAL" in y)))
    return x

for col in succint_table.columns:
    if not col.startswith("event"):
        print(col)
        succint_table[col] = succint_table[col].apply(f)

succint_table = succint_table.rename(columns=col_mapping)

mapping = {"XK01": "Create Vendor (Centrally)", "XK02": "Change Vendor (Centrally)", "FK02": "Change Vendor (Accounting)", "MK02": "Change Vendor (Purchasing)", "VD02": "Change Customer (Sales)", "XD01": "Create Customer (Centrally)", "XD02": "Change Customer (Centrally)", "FD02": "Change Customer (Accounting)", "XD07": "Change Customer Account Group", "FK08": "Confirm Vendor Individually (Acctng)"}

def f1(x):
    return mapping[x]
succint_table["event_activity"] = succint_table["event_activity"].apply(f1)

succint_table.type = "succint"

mdl_exporter.apply(succint_table, "sap_withoutTrial.mdl")

stream = succint_table.to_dict('r')

tgroups = {}

for event in stream:
    event_keys = list(event.keys())
    for key in event_keys:
        if event[key] is None:
            del event[key]
    event_keys = list(event.keys())
    tcode = event["event_tcode"]
    if not tcode in tgroups:
        tgroups[tcode] = set()
    this_list = []
    for col in event_keys:
        if col.startswith("event_table@@"):
            this_list.append(col)
    this_list = tuple(sorted(this_list))
    tgroups[tcode].add(this_list)

print(tgroups)



