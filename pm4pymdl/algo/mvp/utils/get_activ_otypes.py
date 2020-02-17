def get(dataframe, activity, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}
    red_df = dataframe[dataframe["event_activity"] == activity]
    red_df = red_df.dropna(axis=1, how="all")
    cols = [x for x in red_df.columns if not x.startswith("event_")]

    return cols
