def apply(df, model, rel_ev, rel_act, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}
    list_persp = [x for x in df.columns if not x.startswith("event_")]

    for p in list_persp:
        proj_df = df[["event_id", "event_activity", p]].dropna()

        ret[p] = dict(proj_df["event_activity"].value_counts())

    return ret
