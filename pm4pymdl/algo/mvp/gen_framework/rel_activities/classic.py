def apply(df, model, rel_ev_dict, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}
    for persp in rel_ev_dict:
        grouped_df = rel_ev_dict[persp].groupby("event_activity_merge")
        ret[persp] = list(grouped_df.size().to_dict().keys())

    return ret
