import json

import pandas as pd
from dateutil import parser
from lxml import etree, objectify


def apply(file_path, return_obj_df=None, parameters=None):
    if "xml" in file_path:
        return apply_xml(file_path, return_obj_df=return_obj_df, parameters=parameters)
    elif "json" in file_path.lower():
        return apply_json(file_path, return_obj_df=return_obj_df, parameters=parameters)


def apply_xml(file_path, return_obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    from pm4py.objects.petri.importer.variants.pnml import import_net

    parser = etree.XMLParser(remove_comments=True)
    tree = objectify.parse(file_path, parser=parser)
    root = tree.getroot()

    for child in root:
        if child.tag.lower().endswith("events"):
            print("events")
            print(child)
        elif child.tag.lower().endswith("objects"):
            print("objects")
            print(child)

def apply_json(file_path, return_obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    prefix = "ocel:"
    F = open(file_path, "rb")
    obj = json.load(F)
    F.close()
    eve_stream = obj[prefix + "events"]
    for el in eve_stream:
        eve_stream[el]["event_id"] = el
    obj_stream = obj[prefix + "objects"]
    for el in obj_stream:
        obj_stream[el]["object_id"] = el
    obj_stream = list(obj_stream.values())
    obj_type = {}
    for el in obj_stream:
        obj_type[el["object_id"]] = el[prefix + "type"]
        del el[prefix + "type"]
        for k2 in el[prefix + "ovmap"]:
            el["object_" + k2] = el[prefix + "ovmap"][k2]
        del el[prefix + "ovmap"]
    eve_stream = list(eve_stream.values())
    for el in eve_stream:
        new_omap = {}
        for obj in el[prefix + "omap"]:
            typ = obj_type[obj]
            if not typ in new_omap:
                new_omap[typ] = set()
            new_omap[typ].add(obj)
        for typ in new_omap:
            new_omap[typ] = list(new_omap[typ])
        el[prefix + "omap"] = new_omap
        el["event_activity"] = el[prefix + "activity"]
        el["event_timestamp"] = parser.parse(el[prefix + "timestamp"])
        del el[prefix + "activity"]
        del el[prefix + "timestamp"]
        for k2 in el[prefix + "vmap"]:
            el["event_" + k2] = el[prefix + "vmap"][k2]
        del el[prefix + "vmap"]
        for k2 in el[prefix + "omap"]:
            el[k2] = el[prefix + "omap"][k2]
        del el[prefix + "omap"]

    eve_df = pd.DataFrame(eve_stream)
    obj_df = pd.DataFrame(obj_stream)

    eve_df.type = "succint"

    if return_obj_df or (return_obj_df is None and len(obj_df.columns) > 1):
        return eve_df, obj_df
    return eve_df
