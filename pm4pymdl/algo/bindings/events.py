from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl
from collections import Counter
import math
from pm4py.objects.dfg.filtering import dfg_filtering


class Shared:
    diff_values = []


def count_objs(values):
    if type(values) is str and values[0] == "[":
        values = eval(values)
    if type(values) is list:
        return len(values)
    return 0


def apply_bound(value):
    for i in range(len(Shared.diff_values)):
        if value in Shared.diff_values[i]:
            return str(min(Shared.diff_values[i])) + "<=x<=" + str(max(Shared.diff_values[i]))
    # raise Exception("sub error", value, Shared.diff_values)
    return None


def convert_to_stream(df):
    stream = df.to_dict('r')
    for i in range(len(stream)):
        stream_keys = sorted(list(stream[i].keys()))
        for j in range(len(stream_keys)):
            vv = str(stream[i][stream_keys[j]]).lower()
            if vv == "nan" or vv == "nat" or vv == "none" or vv.startswith("0"):
                del stream[i][stream_keys[j]]
    return stream


def get_bindings_from_log(succint_table, max_no_divisions=2, noise_threshold=0.2, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        if succint_table.type == "exploded":
            succint_table = exploded_mdl_to_succint_mdl.apply(succint_table)
    except:
        pass

    obj_count = succint_table.copy()
    OT = [x for x in obj_count.columns if not x.startswith("event_")]

    values_ot = {}
    values_act = {}

    for ot in OT:
        obj_count[ot] = obj_count[ot].apply(count_objs)
        all_values = sorted(list(set(obj_count[ot].dropna().unique())))
        if 0 in all_values:
            del all_values[all_values.index(0)]
        divv = math.ceil(len(all_values) / max_no_divisions)
        diff_values = [[all_values[j] for j in range(len(all_values)) if j // divv == i] for i in
                       range(max_no_divisions)]
        diff_values = [x for x in diff_values if len(x) > 0]
        Shared.diff_values = diff_values
        values_ot[ot] = diff_values
        obj_count[ot] = obj_count[ot].apply(apply_bound)

    stream = convert_to_stream(obj_count)
    activities = set(x["event_activity"] for x in stream)

    acti_dict = {
        x: [{a: b for a, b in y.items() if not a.startswith("event_")} for y in stream if y["event_activity"] == x] for
        x in activities}

    for act in activities:
        values_act[act] = {}

        acti_count = Counter()
        initial_dfg = Counter()

        dic = acti_dict[act]
        for item in dic:
            for key in item:
                acti_count[str((key, item[key]))] += 1
                for other_key in item:
                    if key != other_key:
                        initial_dfg[(str((key, item[key])), str((other_key, item[other_key])))] += 1

        acti_count = dict(acti_count)
        initial_dfg = dict(initial_dfg)

        values_act[act]["acti_count"] = acti_count
        values_act[act]["initial_dfg"] = initial_dfg

        dfg = dfg_filtering.clean_dfg_based_on_noise_thresh(initial_dfg, acti_count, noise_threshold)
        diff_dfg = {x: y for x, y in initial_dfg.items() if x not in dfg}

        values_act[act]["dfg"] = dfg
        values_act[act]["diff_dfg"] = diff_dfg

    ret = {}
    ret["ot"] = values_ot
    ret["act"] = values_act

    return ret

    #print(acti_dict)
