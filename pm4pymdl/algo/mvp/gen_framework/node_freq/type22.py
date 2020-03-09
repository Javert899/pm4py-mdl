from pm4pymdl.algo.mvp.gen_framework.util import filter_on_map
import pandas as pd


def apply(df, model, rel_ev, rel_act, parameters=None):
    if parameters is None:
        parameters = {}

    new_df = filter_on_map.apply(df, model.map)

    df_list = []
    for persp in rel_ev:
        proj_df = df[["event_id", "event_activity", persp]].dropna()
        proj_df = proj_df.groupby(["event_id", persp]).first()
        df_list.append(proj_df)

    if df_list:
        concat_df = pd.concat(df_list)
    else:
        concat_df = pd.DataFrame()

    return dict(concat_df["event_activity"].value_counts())
