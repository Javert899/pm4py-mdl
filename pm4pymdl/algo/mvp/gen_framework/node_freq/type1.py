def apply(df, model, rel_ev, rel_act, parameters=None):
    return dict(df.groupby("event_id").first()["event_activity"].value_counts())
