from pm4py.objects.log.importer.parquet import importer as parquet_importer
from pm4pymdl.algo.mvp.utils import build_graph
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import networkx
import pandas as pd
import dateutil


#dataframe = parquet_importer.apply("accounting.parquet")
#print(len(dataframe))
#df1 = dataframe[dataframe["BLART"].isin(['General document', 'Vendor invoice', 'Vendor payment', 'Vendor credit memo'])]
#print(df1)
#dataframe = dataframe[dataframe["case:concept:name"].isin(df1["case:concept:name"])]
#print(len(dataframe))
#print(dataframe["BLART"].value_counts())
#input()
# dataframe = dataframe.dropna(subset=["BLART"])
#G, conn_comp, timestamps = build_graph.apply(dataframe, "case:concept:name", "AUGBL", "BLART", "time:timestamp")
#log = build_graph.create_log(G, conn_comp, timestamps)
#print(G.subgraph(conn_comp[i]))
#xes_exporter.apply(log, "bkpfgraph.xes")

dataframe = pd.read_csv("VBFA.tsv", sep="\t", dtype={"VBELN": str, "VBELV": str, "VBTYP_N": str, "VBTYP_V": str})
dataframe["time:timestamp"] = dataframe["ERDAT"] + " " + dataframe["ERZET"]
dataframe["time:timestamp"] = pd.to_datetime(dataframe["time:timestamp"], format="%d.%m.%Y %H:%M:%S")
dataframe = dataframe[["VBTYP_N", "VBTYP_V", "VBELN", "VBELV", "time:timestamp"]]
types = dict(dataframe.groupby(["VBELV", "VBTYP_V"]).size())
types = {x[0]: x[1] for x in types}
excl = [x for x in set(dataframe["VBELV"].unique()).difference(set(dataframe["VBELN"].unique())) if str(x).lower() != "nan"]
excl = {x: types[x] for x in excl if x in types}
stream = []
for el in excl:
    stream.append({"VBELN": el, "VBTYP_N": excl[el], "time:timestamp": dateutil.parser.parse("1970-01-01 01:00:00")})
stream_df = pd.DataFrame(stream)
dataframe = pd.concat([dataframe, stream_df])
unq = sorted(list(x for x in dataframe["VBELN"].unique() if str(x) != "nan"))
mapp = {x:x for x in unq}
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
mapp["A"] = "Inquiry"
mapp["B"] = "Quotation"
mapp["C"] = "Order"
mapp["D"] = "Item proposal"
mapp["E"] = "Scheduling agreement"
mapp["F"] = "Scheduling agreement with external service agent"
mapp["G"] = "Contract"
mapp["H"] = "Returns"
mapp["I"] = "Order w / o charge"
mapp["J"] = "Delivery"
mapp["K"] = "Credit memo request"
mapp["L"] = "Debit memo request"
mapp["M"] = "Invoice"
mapp["N"] = "Invoice cancellation"
mapp["O"] = "Credit memo"
mapp["P"] = "Debit memo"
mapp["Q"] = "WMS transfer order"
mapp["R"] = "Goods movement"
mapp["S"] = "Credit memo cancellation"
mapp["T"] = "Returns delivery for order"
mapp["U"] = "Pro forma invoice"
mapp["V"] = "Purchase order"
mapp["W"] = "Independent reqts plan"
mapp["X"] = "Handling unit"
mapp["0"] = "Master contract"
mapp["1"] = "Sales activities (CAS)"
mapp["2"] = "External transaction"
mapp["3"] = "Invoice list"
mapp["4"] = "Credit memo list"
mapp["5"] = "Intercompany invoice"
mapp["6"] = "Intercompany credit memo"
mapp["7"] = "Delivery/shipping notification"
mapp["8"] = "Shipment"
mapp["a"] = "Shipment costs"
mapp["e"] = "Allocation table"
mapp["g"] = "Rough Goods Receipt"
mapp["h"] = "Cancel goods issue"
mapp["i"] = "Goods receipt"
mapp["j"] = "JIT call"
"""
0 Master contract
1 Sales activities (CAS)
2 External transaction
3 Invoice list
4 Credit memo list
5 Intercompany invoice
6 Intercompany credit memo
7 Delivery/shipping notification
8 Shipment
a Shipment costs
e Allocation table
g Rough Goods Receipt (only IS-Retail)
h Cancel goods issue
i Goods receipt
j JIT call
"""
dataframe["VBTYP_N"] = dataframe["VBTYP_N"].map(mapp)

G, conn_comp, timestamps = build_graph.apply(dataframe, "VBELN", "VBELV", "VBTYP_N", "time:timestamp", reverse=True)
print("A")
log = build_graph.create_log(G, conn_comp, timestamps)
print("B")
xes_exporter.apply(log, "vbfagraph.xes")
print("C")
"""
print(dataframe.columns)
doc_types = dataframe[["case:concept:name", "BLART"]].to_dict('r')
doc_types = {str(x["case:concept:name"]): str(x["BLART"]) for x in doc_types if x["BLART"] is not None}
# print(doc_types)
df2 = dataframe.dropna(subset=["case:concept:name", "AUGBL"])
corr = dict(df2.groupby(["case:concept:name", "AUGBL"]).size())
corr = {str(x[0]): str(x[1]) for x in corr if str(x[0]) in doc_types and str(x[1]) in doc_types}
corr = {doc_types[x] + "=" + x: doc_types[y] + "=" + y for x, y in corr.items()}
print(corr)
"""
