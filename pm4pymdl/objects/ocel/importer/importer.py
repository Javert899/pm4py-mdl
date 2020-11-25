import json

import pandas as pd
from dateutil import parser
from lxml import etree, objectify
import dateutil


def apply(file_path, return_obj_df=None, parameters=None):
    if "xml" in file_path:
        return apply_xml(file_path, return_obj_df=return_obj_df, parameters=parameters)
    elif "json" in file_path.lower():
        return apply_json(file_path, return_obj_df=return_obj_df, parameters=parameters)


def parse_xml(value, tag_str_lower):
    if "float" in tag_str_lower:
        return float(value)
    elif "date" in tag_str_lower:
        return dateutil.parser.parse(value)
    return str(value)


def apply_xml(file_path, return_obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    parser = etree.XMLParser(remove_comments=True)
    tree = objectify.parse(file_path, parser=parser)
    root = tree.getroot()

    eve_stream = []
    obj_stream = []
    obj_types = {}

    for child in root:
        if child.tag.lower().endswith("events"):
            for event in child:
                eve = {}
                for child2 in event:
                    if child2.get("key") == "id":
                        eve["event_id"] = child2.get("value")
                    elif child2.get("key") == "timestamp":
                        eve["event_timestamp"] = dateutil.parser.parse(child2.get("value"))
                    elif child2.get("key") == "activity":
                        eve["event_activity"] = child2.get("value")
                    elif child2.get("key") == "omap":
                        omap = []
                        for child3 in child2:
                            omap.append(child3.get("value"))
                        eve["@@omap"] = omap
                    elif child2.get("key") == "vmap":
                        for child3 in child2:
                            eve["event_" + child3.get("key")] = parse_xml(child3.get("value"), child3.tag.lower())
                eve_stream.append(eve)

        elif child.tag.lower().endswith("objects"):
            for object in child:
                obj = {}
                for child2 in object:
                    if child2.get("key") == "id":
                        obj["object_id"] = child2.get("value")
                    elif child2.get("key") == "type":
                        obj["object_type"] = child2.get("value")
                    elif child2.get("key") == "ovmap":
                        for child3 in child2:
                            obj["object_" + child3.get("key")] = parse_xml(child3.get("value"), child3.tag.lower())
                obj_stream.append(obj)

    for obj in obj_stream:
        obj_types[obj["object_id"]] = obj["object_type"]

    for eve in eve_stream:
        for obj in eve["@@omap"]:
            ot = obj_types[obj]
            if not ot in eve:
                eve[ot] = []
            eve[ot].append(obj)
        del eve["@@omap"]

    eve_df = pd.DataFrame(eve_stream)
    obj_df = pd.DataFrame(obj_stream)

    eve_df.type = "succint"

    if return_obj_df or (return_obj_df is None and len(obj_df.columns) > 1):
        return eve_df, obj_df
    return eve_df



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
        el["object_type"] = el[prefix + "type"]
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
