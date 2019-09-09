import pandas as pd


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    perspectives = [x for x in df.columns if not x.startswith("event_")]

    ret = {}
    basic_columns = ["event_id", "event_activity"]

    for c in perspectives:
        proj_df = df[basic_columns + [c]].dropna()

        first_df = proj_df.groupby(c).first()

        merged_df = first_df.merge(proj_df, how='inner', left_on=c, right_on=c, suffixes=('', '_2'))
        merged_df = merged_df[merged_df["event_id"] != merged_df["event_id_2"]]
        merged_df["event_activity_merge"] = merged_df["event_activity"] + "@@" + merged_df["event_activity_2"]

        merged_df = merged_df.reset_index()

        ret[c] = merged_df

    return ret
