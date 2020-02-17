def get(dataframe, activity, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}

    red_df = dataframe[dataframe["event_activity"] == activity]
    red_df = red_df.groupby("event_id").first()
    red_df = red_df.dropna(axis=1, how="all")
    cols = [x for x in red_df if x.startswith("event_") and not x in ["event_id", "event_activity", "event_timestamp"]]
    for c in cols:
        ret[c] = str(red_df[c].dtype)

    return ret
