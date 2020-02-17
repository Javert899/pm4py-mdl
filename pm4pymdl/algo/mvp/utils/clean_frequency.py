from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl

def apply(df, min_acti_freq=0):
    try:
        if df.type == "succint":
            df = succint_mdl_to_exploded_mdl.apply(df)
    except:
        pass
    activ = dict(df.groupby("event_id").first()["event_activity"].value_counts())
    activ = [x for x,y in activ.items() if y >= min_acti_freq]
    return df[df["event_activity"].isin(activ)]
