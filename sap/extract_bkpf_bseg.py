import pandas as pd
from pm4pymdl.algo.mvp.discovery import factory as mvp_disc_factory
from pm4pymdl.visualization.mvp import factory as mvp_vis_factory

df1 = pd.read_csv("bkpf.csv")
renaming = {"tcode": "event_activity", "id": "event_id", "budat": "event_timestamp", "xblnr": "xblnr", "belnr": "belnr", "usnam": "usnam"}
ec = ["event_activity", "event_id", "event_timestamp"]
ots = ["xblnr", "belnr", "usnam"]
#ots = ["caseid"]
df1 = df1[list(renaming.keys())].rename(columns=renaming)
#df1 = df1[df1["belnr"] != "* TRIAL * "]
list_df1 = []
for ot in ots:
    ac = ec + [ot]
    list_df1.append(df1[ac].dropna())
df1 = pd.concat(list_df1)
df2 = pd.read_csv("bseg.csv")
renaming_y = {"ktosl": "event_activity", "augdt": "event_timestamp", "id": "event_id", "belnr": "belnr", "hkont": "hkont", "dmbtr": "event_dmbtr", "wrbtr": "event_wrbtr", "mwsts": "event_mwsts", "hwbas": "event_hwbas", "fwbas": "event_fwbas"}
ots = ["belnr", "hkont"]
df2 = df2[list(renaming_y.keys())].rename(columns=renaming_y)
df2 = df2[df2["belnr"] != "* TRIAL * "]
df2 = df2[df2["hkont"] != "* TRIAL * "]

print(df2.columns)
print(df1.columns)

print(len(df1))
df1 = df1.merge(df2, left_on="belnr", right_on="belnr", suffixes=('', '_y'), how="left")
df1 = df1[list(renaming.values())+["hkont", "event_dmbtr", "event_wrbtr", "event_mwsts", "event_hwbas", "event_fwbas"]]
print(len(df1))

print(df1)

list_df2 = []
for ot in ots:
    ac = ec + [ot]
    list_df2.append(df2[ac].dropna())
df2 = pd.concat(list_df2)
df = pd.concat([df1, df2])
df = df1
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
df = df.sort_values(["event_timestamp", "event_id"])
df.type = "exploded"

mapping = {}
mapping["FB01"] = "Post Document (FB01)"
mapping["AB01"] = "Create asset transactions"
mapping["FB08"] = "Reversal"
mapping["F110"] = "Parameters for Automatic Payment"
mapping["FOSA"] = "Execute debit position"
mapping["FB05"] = "Post with Clearing"
mapping["MB01"] = "Post Goods Receipt"
mapping["VF11"] = "Cancel Billing Document"
mapping["VF04"] = "Maintain Billing Due List"
mapping["VL02"] = "Change Outbound Delivery"
mapping["MR01"] = "Process Incoming Invoice"
mapping["FBR2"] = "Post Document (FBR2)"
mapping["FBZ1"] = "Post Incoming Payments"
mapping["MR1M"] = "Direct posting to G/L Account"
mapping["MB1A"] = "Goods Withdrawal"
mapping["MR21"] = "Price Change"
mapping["VF02"] = "Change Billing Document"
mapping["VF01"] = "Create Billing Document"
mapping["MB1C"] = "Other Goods Receipts"
mapping["MBST"] = "Cancel Material Document"
mapping["KO8G"] = "Act. Settlment"
mapping["ABF1"] = "Post Document (ABF1)"
mapping["VL01"] = "Create Delivery"
#mapping["KBS"] = ""
mapping["SKV"] = "Cash discount clearing (net method)"
mapping["CK21"] = "Release the cost estimate"
mapping["FBA7"] = "Post Vendor Down Payment"
mapping["FBA6"] = "Vendor Down Payment Request"
mapping["FB10"] = "Invoice/Credit Fast Entry"
mapping["IW41"] = "Enter PM Order Confirmation"
mapping["KO88"] = "Actual Settlement: Order"
mapping["CJ88"] = "Settle Projects and Networks"
mapping["CO11"] = "Enter Time Ticket"
mapping["MB31"] = "Goods Receipt for Production Order"
mapping["FBM1"] = "Enter Sample Document"
mapping["COGI"] = "Postprocess Faulty Goods Movements"
mapping["FOSH"] = "Vacancy debit position"
mapping["FOUA"] = "Calculate sales settlement"
mapping["MF40"] = "Final backflush for make-to-stock production"
mapping["FBA8"] = "Clear Vendor Down Payment"
mapping["MB11"] = "Goods Movement"
mapping["MBSL"] = "Copy Material Document"
mapping["MB0A"] = "Post Goods Receipt for PO"
mapping["FOB6"] = "Input tax distribution"
mapping["FB1S"] = "Clear G/L Account"
mapping["WRX"] = "Account determination for GR/IR clearing account"
mapping["GBB"] = "Offsetting entry for inventory posting"
mapping["MB1B"] = "Enter Transfer Posting"
df["event_activity"] = df["event_activity"].apply(lambda x: mapping[x])

from pm4pymdl.objects.mdl.exporter import factory as mdl_exporter
mdl_exporter.apply(df, "bkpf_bseg.mdl")

#print(df["event_activity"].unique())
#input()
#print(df)

model = mvp_disc_factory.apply(df, parameters={"min_dfg_occurrences": 3, "performance": False, "decreasing_factor_sa_ea": 0.0, "dependency_thresh": 0.3, "perspectives": ["belnr", "xblnr", "hkont"]})

gviz = mvp_vis_factory.apply(model, parameters={"format": "svg"})
mvp_vis_factory.save(gviz, "bkpf_caseid_frequency.svg")

model = mvp_disc_factory.apply(df, parameters={"min_dfg_occurrences": 3, "performance": True, "decreasing_factor_sa_ea": 0.0, "dependency_thresh": 0.3, "perspectives": ["belnr", "xblnr", "hkont"]})

gviz = mvp_vis_factory.apply(model, parameters={"format": "svg"})
mvp_vis_factory.save(gviz, "bkpf_caseid_performance.svg")

#gviz = mvp_vis_factory.apply(model)
#mvp_vis_factory.view(gviz)

#df["case:concept:name"] = df["caseid"]
#df["concept:name"] = df["event_activity"]
#df["time:timestamp"] = df["event_timestamp"]

#from pm4py.algo.discovery.inductive import factory as inductive_miner

#net, im, fm = inductive_miner.apply(df)

#from pm4py.visualization.petrinet import factory as pn_vis_factory

#gviz = pn_vis_factory.apply(net, im, fm)
#pn_vis_factory.view(gviz)

