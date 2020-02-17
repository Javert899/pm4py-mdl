from pm4pymdl.algo.mvp.utils import filter_metaclass
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl


def filter_float(df, act, attr, v1, v2, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        if df.type == "succint":
            df = succint_mdl_to_exploded_mdl.apply(df)
    except:
        pass

    red_df = df[df["event_activity"] == act]
    red_df = red_df[df[attr] >= v1]
    red_df = red_df[df[attr] <= v2]

    red_df = red_df.dropna(how="all", axis=1)

    return filter_metaclass.do_filtering(df, red_df, parameters=parameters)

