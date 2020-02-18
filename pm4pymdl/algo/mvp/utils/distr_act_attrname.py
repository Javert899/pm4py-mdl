def get(dataframe, activity, attr_name, parameters=None):
    if parameters is None:
        parameters = {}

    red_df = dataframe[dataframe["event_activity"] == activity]
    return dict(red_df[attr_name].describe())
