from collections import Counter


def get(dataframe, activity, ot, parameters=None):
    if parameters is None:
        parameters = {}

    red_df = dataframe[dataframe["event_activity"] == activity]
    red_df = red_df.dropna(axis=1, how="all")
    red_df = red_df.dropna(subset=[ot])

    lista = [y for x,y in dict(red_df.groupby("event_id").size()).items()]

    return dict(Counter(lista))

