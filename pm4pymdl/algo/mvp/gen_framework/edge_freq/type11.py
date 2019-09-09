def apply(df, model, rel_ev, rel_act, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}

    for persp in rel_ev:
        df = rel_ev[persp]
        df = df[df["event_activity_merge"].isin(rel_act[persp])]
        df = df.groupby(["event_id", "event_id_2"]).first()

        r = df.groupby("event_activity_merge").size().to_dict()

        ret[persp] = r

    return ret
