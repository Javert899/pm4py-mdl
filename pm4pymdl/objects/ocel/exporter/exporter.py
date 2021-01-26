import json

import pandas as pd

from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if "time" in str(type(obj)):
        return obj.isoformat()
    return str(obj)


def get_type(t0):
    if "float" in str(t0).lower() or "double" in str(t0).lower():
        return "float"
    elif "object" in str(t0).lower():
        return "string"
    else:
        return "string"


def apply(df, file_path, obj_df=None, parameters=None):
    if "xml" in file_path.lower():
        return apply_xml(df, file_path, obj_df=obj_df, parameters=parameters)
    elif "json" in file_path.lower():
        return apply_json(df, file_path, obj_df=obj_df, parameters=parameters)


def apply_json(df, file_path, obj_df=None, parameters=None):
    ret = get_python_obj(df, obj_df=obj_df, parameters=parameters)
    F = open(file_path, "w")
    json.dump(ret, F, default=json_serial, indent=2)
    F.close()


def apply_xml(df, file_path, obj_df=None, parameters=None):
    ret = get_python_obj(df, obj_df=obj_df, parameters=parameters)
    from lxml import etree

    prefix = "ocel:"

    root = etree.Element("log")
    global_event = etree.SubElement(root, "global")
    global_event.set("scope", "event")
    for k, v in ret[prefix + "global-event"].items():
        child = etree.SubElement(global_event, "string")
        child.set("key", k.split(prefix)[-1])
        child.set("value", v)
    global_object = etree.SubElement(root, "global")
    global_object.set("scope", "object")
    for k, v in ret[prefix + "global-object"].items():
        child = etree.SubElement(global_object, "string")
        child.set("key", k.split(prefix)[-1])
        child.set("value", v)
    global_log = etree.SubElement(root, "global")
    global_log.set("scope", "log")
    attribute_names = etree.SubElement(global_log, "list")
    attribute_names.set("key", "attribute-names")
    object_types = etree.SubElement(global_log, "list")
    object_types.set("key", "object-types")
    for k in ret[prefix + "global-log"][prefix + "attribute-names"]:
        subel = etree.SubElement(attribute_names, "string")
        subel.set("key", "attribute-name")
        subel.set("value", k)
    for k in ret[prefix + "global-log"][prefix + "object-types"]:
        subel = etree.SubElement(object_types, "string")
        subel.set("key", "object-type")
        subel.set("value", k)
    version = etree.SubElement(global_log, "string")
    version.set("key", "version")
    version.set("value", ret[prefix + "global-log"][prefix + "version"])
    ordering = etree.SubElement(global_log, "string")
    ordering.set("key", "ordering")
    ordering.set("value", ret[prefix + "global-log"][prefix + "ordering"])
    events = etree.SubElement(root, "events")
    for k, v in ret[prefix + "events"].items():
        event = etree.SubElement(events, "event")
        event_id = etree.SubElement(event, "string")
        event_id.set("key", "id")
        event_id.set("value", str(k))
        event_activity = etree.SubElement(event, "string")
        event_activity.set("key", "activity")
        event_activity.set("value", v[prefix + "activity"])
        event_timestamp = etree.SubElement(event, "date")
        event_timestamp.set("key", "timestamp")
        event_timestamp.set("value", v[prefix + "timestamp"].isoformat())
        event_omap = etree.SubElement(event, "list")
        event_omap.set("key", "omap")
        for k2 in v[prefix + "omap"]:
            obj = etree.SubElement(event_omap, "string")
            obj.set("key", "object-id")
            obj.set("value", k2)
        event_vmap = etree.SubElement(event, "list")
        event_vmap.set("key", "vmap")
        for k2, v2 in v[prefix + "vmap"].items():
            attr = etree.SubElement(event_vmap, get_type(df["event_" + k2].dtype))
            attr.set("key", k2)
            attr.set("value", str(v2))
    objects = etree.SubElement(root, "objects")
    for k, v in ret[prefix + "objects"].items():
        object = etree.SubElement(objects, "object")
        object_id = etree.SubElement(object, "string")
        object_id.set("key", "id")
        object_id.set("value", str(k))
        object_type = etree.SubElement(object, "string")
        object_type.set("key", "type")
        object_type.set("value", v[prefix + "type"])
        object_ovmap = etree.SubElement(object, "list")
        object_ovmap.set("key", "ovmap")
        for k2, v2 in v[prefix + "ovmap"].items():
            if str(v2).lower() != "nan" and str(v2).lower() != "nat":
                object_att = etree.SubElement(object_ovmap, get_type(obj_df["object_" + k2].dtype))
                object_att.set("key", k2)
                object_att.set("value", str(v2))

    tree = etree.ElementTree(root)
    tree.write(file_path, pretty_print=True, xml_declaration=True, encoding="utf-8")


def get_python_obj(df, obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    prefix = "ocel:"

    conversion_needed = True
    try:
        if df.type == "succint":
            conversion_needed = False
    except:
        pass

    if conversion_needed:
        df = exploded_mdl_to_succint_mdl.apply(df)

    if obj_df is not None:
        df = pd.concat([df, obj_df])

    col = list(df.columns)
    rep_dict = {}
    obj_types = set()

    for x in col:
        if x.startswith("event_"):
            rep_dict[x] = x.split("event_")[1]
        else:
            y = "case_" + x
            rep_dict[x] = y
            obj_types.add(x)
    df = df.rename(columns=rep_dict)
    df = df.dropna(subset=["id"])
    df = df.dropna(subset=["activity"])
    df = df.dropna(subset=["timestamp"])
    df["id"] = df["id"].astype(str)

    activities = list(df["activity"].unique())

    acti_df = {}
    ot_df = {}

    acti_mandatory = {}
    ot_mandatory = {}
    objects_from_df_type = {}

    att_types = {}

    for act in activities:
        red_df = df[df["activity"] == act].dropna(how="all", axis=1)
        red_df2 = red_df.dropna(how="any", axis=1)
        acti_df[act] = red_df

        for col in red_df.columns:
            if not col.startswith("case_") and not col in ["id", "activity", "timestamp"]:
                att_types[col] = get_type(red_df[col].dtype)

        acti_mandatory[act] = []
        for col in red_df2.columns:
            if not col.startswith("case_") and not col in ["id", "activity", "timestamp"]:
                acti_mandatory[act].append(col)

    if obj_df is not None:
        OT = obj_df["object_type"].dropna().unique()

        for ot in OT:
            obj_types.add(ot)

            red_df = obj_df[obj_df["object_type"] == ot].dropna(how="all", axis=1)

            red_df = red_df.dropna(subset=["object_id"]).drop(columns=["object_type"])

            col = list(red_df.columns)
            rep_dict = {}
            for x in col:
                rep_dict[x] = x.split("object_")[-1]
            red_df = red_df.rename(columns=rep_dict)

            for col in red_df.columns:
                if not col in ["id", "type"]:
                    att_types[col] = get_type(red_df[col].dtype)

            red_df2 = red_df.dropna(how="any", axis=1)

            ot_mandatory[ot] = []
            for col in red_df2.columns:
                if not col in ["id"]:
                    # type_mandatory[ot][col] = get_type(red_df2[col].dtype)
                    ot_mandatory[ot].append(col)

            ot_df[ot] = red_df

    ret = {}
    att_names = sorted(list(set(att_types.keys())))
    att_typ_values = sorted(list(set(att_types.values())))
    object_types = sorted(list(obj_types))
    ret[prefix + "global-event"] = {prefix + "activity": "__INVALID__"}
    ret[prefix + "global-object"] = {prefix + "type": "__INVALID__"}
    ret[prefix + "global-log"] = {prefix + "attribute-names": att_names,
                                  prefix + "object-types": [x for x in object_types if not x.startswith("object_")]}
    # ret[prefix+"types_corr"] = att_types
    ret[prefix + "events"] = {}
    ret[prefix + "objects"] = {}
    # ret[prefix+"att_mand"] = acti_mandatory
    # ret[prefix+"ot_mand"] = ot_mandatory

    stream = df.dropna(how="all", axis=1).to_dict("r")
    for el in stream:
        el2 = {}
        el2[prefix + "activity"] = el["activity"]
        el2[prefix + "timestamp"] = el["timestamp"]
        el2[prefix + "omap"] = {}
        el2[prefix + "vmap"] = {}

        for k in el:
            try:
                if el[k] is not None and str(el[k]).lower() != "nan" and str(el[k]).lower() != "nat":
                    if k.startswith("case_"):
                        if type(el[k]) is str:
                            y = eval(el[k])
                        else:
                            y = el[k]
                        if y:
                            if len(y) > 1 or len(y[0]) > 0:
                                el2[prefix + "omap"][k.split("case_")[1]] = y
                            for subel in y:
                                objects_from_df_type[subel] = k.split("case_")[1]
                    elif not k in ["id", "activity", "timestamp"]:
                        el2[prefix + "vmap"][k] = el[k]
            except:
                pass
        for x, y in el2[prefix + "omap"].items():
            if type(y) is list and type(y[0]) is list:
                el2[prefix + "omap"][x] = y[0]
        el2[prefix + "omap"] = list(set(z for y in el2[prefix + "omap"].values() for z in y))
        ret[prefix + "events"][el["id"]] = el2

    for t in ot_df:
        stream = ot_df[t].to_dict('r')
        for el in stream:
            el2 = {}
            el2[prefix + "type"] = t
            el2[prefix + "ovmap"] = {}
            for k in el:
                if not k in ["id", "type"]:
                    el2[prefix + "ovmap"][k] = el[k]
            ret[prefix + "objects"][el["id"]] = el2
    for o in objects_from_df_type:
        if not o in ret[prefix + "objects"]:
            ret[prefix + "objects"][o] = {prefix + "type": objects_from_df_type[o], prefix + "ovmap": {}}

    # ret[prefix + "version"] = "0.1"
    ret[prefix + "global-log"][prefix + "version"] = "1.0"
    ret[prefix + "global-log"][prefix + "ordering"] = "timestamp"

    print("events = ", len(ret[prefix + "events"]))
    print("objects = ", len(ret[prefix + "objects"]))

    return ret
