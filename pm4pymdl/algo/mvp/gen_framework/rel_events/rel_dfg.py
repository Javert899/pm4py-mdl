import pandas as pd


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}
    columns = [x for x in df.columns if not x.startswith("event_")]
    basic_columns = ["event_id", "event_activity"]
    for col in columns:
        proj_df = df[basic_columns + [col]].dropna()
        proj_df["@@index"] = proj_df.index
        proj_df = proj_df.sort_values([col, "@@index"])
        proj_df_shift = proj_df.shift(-1)
        proj_df_shift.columns = [str(c) + '_2' for c in proj_df_shift.columns]
        concat_df = pd.concat([proj_df, proj_df_shift], axis=1)
        concat_df = concat_df[concat_df[col] == concat_df[col+"_2"]]

        concat_df["event_activity_merge"] = concat_df["event_activity"] + "@@" + concat_df["event_activity_2"]

        ret[col] = concat_df

    return ret
