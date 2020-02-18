from pm4pymdl.algo.mvp.utils import filter_metaclass
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl


def filter_ot(df, act, ot, v1, v2, parameters=None):
    if parameters is None:
        parameters = {}

    print(len(df))

    try:
        if df.type == "succint":
            df = succint_mdl_to_exploded_mdl.apply(df)
    except:
        pass

    red_df0 = df[df["event_activity"] == act]
    red_df = red_df0.dropna(subset=[ot])

    dct = red_df.groupby("event_id").size().to_dict()
    lst = [x for x,y in dct.items() if v1 <= y <= v2]

    red_df = red_df0[red_df0["event_id"].isin(lst)]

    return filter_metaclass.do_filtering(df, red_df, parameters=parameters)

