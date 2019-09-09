from pm4pymdl.algo.mvp.gen_framework.util import filter_on_map


def apply(df, model, rel_ev, rel_act, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}

    for persp in rel_ev:
        df = rel_ev[persp]
        df = df[df["event_activity_merge"].isin(rel_act[persp])]
        df = filter_on_map.apply(df, model.map, second_required=True, timestamp_required=False, merge_required=True)
        df = df.groupby([persp, persp+"_2", "event_activity_merge"]).first()
        r = df.groupby("event_activity_merge").size().to_dict()

        ret[persp] = r

    return ret
