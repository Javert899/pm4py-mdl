from pm4pymdl.algo.mvp.gen_framework.util import filter_on_map

def apply(df, model, rel_ev, rel_act, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}

    for persp in rel_ev:
        df = rel_ev[persp]
        df = df[df["event_activity_merge"].isin(rel_act[persp])]
        try:
            df = filter_on_map.apply(df, model.map, second_required=True, timestamp_required=False, merge_required=True)
            df = df.groupby(["event_id", "event_id_2"]).first()

            r = df.groupby("event_activity_merge").size().to_dict()

            ret[persp] = r
        except:
            pass

    return ret
