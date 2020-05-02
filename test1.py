import pandas as pd
from pm4py.objects.log.importer.xes import factory as importer
from pm4py.objects.conversion.log import factory as log_conversion
from frozendict import frozendict
import uuid
from copy import deepcopy
from pm4pymdl.objects.mdl.exporter import factory as mdl_exporter

N = 10

def read_permit_log():
    log = importer.apply("PermitLog.xes_", variant="nonstandard", parameters={"max_no_traces_to_import": N})
    events = set()
    for trace in log:
        diff_attrs = set()
        for attr in trace.attributes:
            if trace.attributes[]
            if "DeclarationNumber" in attr:
                diff_attrs.add(("DECLARATION", trace.attributes[attr]))
            if "travel permit number" in attr:
                diff_attrs.add(("TRAVEL_PERMIT", trace.attributes[attr]))
            if "RfpNumber" in attr:
                diff_attrs.add(("RF_PAYMENT", trace.attributes[attr]))
        for ev in trace:
            nev0 = {}
            nev0["event_activity"] = ev["concept:name"]
            nev0["event_timestamp"] = ev["time:timestamp"]
            nev0["event_id"] = ev["id"]
            nev0["event_resource"] = ev["org:resource"]
            nev0["event_role"] = ev["org:role"]
            for attr in diff_attrs:
                nev = deepcopy(nev0)
                nev[attr[0]] = attr[1]
                events.add(frozendict(nev))
    return events

def read_domestic_declarations():
    log = importer.apply("DomesticDeclarations.xes_", variant="nonstandard", parameters={"max_no_traces_to_import": N})
    events = set()
    for trace in log:
        diff_attrs = set()
        for attr in trace.attributes:
            if "DeclarationNumber" in attr:
                diff_attrs.add(("DECLARATION", trace.attributes[attr]))
            if "travel permit number" in attr:
                diff_attrs.add(("TRAVEL_PERMIT", trace.attributes[attr]))
            if "RfpNumber" in attr:
                diff_attrs.add(("RF_PAYMENT", trace.attributes[attr]))
        for ev in trace:
            nev0 = {}
            nev0["event_activity"] = ev["concept:name"]
            nev0["event_timestamp"] = ev["time:timestamp"]
            nev0["event_id"] = ev["id"]
            nev0["event_resource"] = ev["org:resource"]
            nev0["event_role"] = ev["org:role"]
            for attr in diff_attrs:
                nev = deepcopy(nev0)
                nev[attr[0]] = attr[1]
                events.add(frozendict(nev))
    return events


def read_international_declarations():
    log = importer.apply("InternationalDeclarations.xes_", variant="nonstandard", parameters={"max_no_traces_to_import": N})
    events = set()
    for trace in log:
        diff_attrs = set()
        for attr in trace.attributes:
            if "DeclarationNumber" in attr:
                diff_attrs.add(("DECLARATION", trace.attributes[attr]))
            if "travel permit number" in attr:
                diff_attrs.add(("TRAVEL_PERMIT", trace.attributes[attr]))
            if "RfpNumber" in attr:
                diff_attrs.add(("RF_PAYMENT", trace.attributes[attr]))
        for ev in trace:
            nev0 = {}
            nev0["event_activity"] = ev["concept:name"]
            nev0["event_timestamp"] = ev["time:timestamp"]
            nev0["event_id"] = ev["id"]
            nev0["event_resource"] = ev["org:resource"]
            nev0["event_role"] = ev["org:role"]
            for attr in diff_attrs:
                nev = deepcopy(nev0)
                nev[attr[0]] = attr[1]
                events.add(frozendict(nev))
    return events

def read_req_payments():
    log = importer.apply("RequestForPayment.xes_", variant="nonstandard", parameters={"max_no_traces_to_import": N})
    events = set()
    for trace in log:
        diff_attrs = set()
        for attr in trace.attributes:
            if "DeclarationNumber" in attr:
                diff_attrs.add(("DECLARATION", trace.attributes[attr]))
            if "travel permit number" in attr:
                diff_attrs.add(("TRAVEL_PERMIT", trace.attributes[attr]))
            if "RfpNumber" in attr:
                diff_attrs.add(("RF_PAYMENT", trace.attributes[attr]))
        for ev in trace:
            nev0 = {}
            nev0["event_activity"] = ev["concept:name"]
            nev0["event_timestamp"] = ev["time:timestamp"]
            nev0["event_id"] = ev["id"]
            nev0["event_resource"] = ev["org:resource"]
            nev0["event_role"] = ev["org:role"]
            for attr in diff_attrs:
                nev = deepcopy(nev0)
                nev[attr[0]] = attr[1]
                events.add(frozendict(nev))
    return events


def read_prep_costs():
    log = importer.apply("PrepaidTravelCost.xes_", variant="nonstandard", parameters={"max_no_traces_to_import": N})
    events = set()
    for trace in log:
        diff_attrs = set()
        for attr in trace.attributes:
            if "DeclarationNumber" in attr:
                diff_attrs.add(("DECLARATION", trace.attributes[attr]))
            if "travel permit number" in attr:
                diff_attrs.add(("TRAVEL_PERMIT", trace.attributes[attr]))
            if "RfpNumber" in attr:
                diff_attrs.add(("RF_PAYMENT", trace.attributes[attr]))
        for ev in trace:
            nev0 = {}
            nev0["event_activity"] = ev["concept:name"]
            nev0["event_timestamp"] = ev["time:timestamp"]
            nev0["event_id"] = ev["id"]
            nev0["event_resource"] = ev["org:resource"]
            nev0["event_role"] = ev["org:role"]
            for attr in diff_attrs:
                nev = deepcopy(nev0)
                nev[attr[0]] = attr[1]
                events.add(frozendict(nev))
    return events

if __name__ == "__main__":
    permit_log = read_permit_log()
    domestic_decl = read_domestic_declarations()
    intern_declarations = read_international_declarations()
    req_payment = read_req_payments()
    prepaid_cost = read_prep_costs()

    log = permit_log.union(domestic_decl).union(intern_declarations).union(req_payment).union(prepaid_cost)
    log = [dict(x) for x in log]
    log = sorted(log, key=lambda x: x["event_timestamp"])
    print(len(log))
    df = pd.DataFrame(log)
    df.type = "exploded"
    print(df)
    mdl_exporter.apply(df, "bpic2020.mdl")
