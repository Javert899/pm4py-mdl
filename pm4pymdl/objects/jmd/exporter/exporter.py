from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl, succint_mdl_to_exploded_mdl
import pandas as pd
import json


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if "time" in str(type(obj)):
        stru = str(obj)
        stru = stru.replace(" ","T") + "Z"
        return stru
    return str(obj)


def get_type(t0):
    if "float" in str(t0).lower() or "double" in str(t0).lower():
        return "double"
    elif "object" in str(t0).lower():
        return "string"
    else:
        return "string"


def apply(df, file_path, obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    prefix = "jmd:"

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
                rep_dict[x] = x.split("object_")[1]
            red_df = red_df.rename(columns=rep_dict)

            for col in red_df.columns:
                if not col in ["id", "type"]:
                    att_types[col] = get_type(red_df[col].dtype)

            red_df2 = red_df.dropna(how="any", axis=1)

            ot_mandatory[ot] = []
            for col in red_df2.columns:
                if not col in ["id"]:
                    #type_mandatory[ot][col] = get_type(red_df2[col].dtype)
                    ot_mandatory[ot].append(col)

            ot_df[ot] = red_df

    ret = {}
    att_names = sorted(list(set(att_types.keys())))
    att_typ_values = sorted(list(set(att_types.values())))
    object_types = sorted(list(obj_types))
    ret[prefix+"att_names"] = att_names
    ret[prefix+"att_types"] = att_typ_values
    ret[prefix+"object_types"] = object_types
    ret[prefix+"types_corr"] = att_types
    ret[prefix+"events"] = {}
    ret[prefix+"objects"] = {}
    ret[prefix+"att_mand"] = acti_mandatory
    ret[prefix+"ot_mand"] = ot_mandatory

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
        el2[prefix + "omap"] = list(set(z for y in el2[prefix+"omap"].values() for z in y))
        ret[prefix + "events"][el["id"]] = el2

    for t in ot_df:
        stream = ot_df[t].to_dict('r')
        for el in stream:
            el2 = {}
            el2[prefix + "otyp"] = t
            el2[prefix + "ovmap"] = {}
            for k in el:
                if not k in ["id", "type"]:
                    el2[prefix + "ovmap"][k] = el[k]
            ret[prefix + "objects"][el["id"]] = el2
    for o in objects_from_df_type:
        if not o in ret[prefix + "objects"]:
            ret[prefix + "objects"][o] = {prefix+"otyp": objects_from_df_type[o], prefix+"ovmap": {}}
    F = open(file_path, "w")
    json.dump(ret, F, default=json_serial, indent=2)
    F.close()
