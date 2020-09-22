import json
import pandas as pd
from dateutil import parser


def apply(file_path, return_obj_df=True, parameters=None):
    if parameters is None:
        parameters = {}

    prefix = "jmd:"
    F = open(file_path, "rb")
    obj = json.load(F)
    F.close()
    eve_stream = obj[prefix+"events"]
    for el in eve_stream:
        eve_stream[el]["event_id"] = el
    eve_stream = list(eve_stream.values())
    for el in eve_stream:
        el["event_activity"] = el[prefix+"activity"]
        el["event_timestamp"] = parser.parse(el[prefix+"timestamp"])
        del el[prefix+"activity"]
        del el[prefix+"timestamp"]
        for k2 in el[prefix+"vmap"]:
            el["event_"+k2] = el[prefix+"vmap"][k2]
        del el[prefix+"vmap"]
        for k2 in el[prefix+"omap"]:
            el[k2] = el[prefix+"omap"][k2]
        del el[prefix + "omap"]
    obj_stream = obj[prefix+"objects"]
    for el in obj_stream:
        obj_stream[el]["object_id"] = el
    obj_stream = list(obj_stream.values())
    for el in obj_stream:
        el["object_type"] = el[prefix+"otyp"]
        del el[prefix+"otyp"]
        for k2 in el[prefix+"ovmap"]:
            el["object_"+k2] = el[prefix+"ovmap"][k2]
        del el[prefix+"ovmap"]

    eve_df = pd.DataFrame(eve_stream)
    obj_df = pd.DataFrame(obj_stream)

    if return_obj_df:
        return eve_df, obj_df
    return eve_df


