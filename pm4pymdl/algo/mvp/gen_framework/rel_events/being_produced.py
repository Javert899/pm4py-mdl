import pandas as pd


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    ret = {}
    columns = [x for x in df.columns if not x.startswith("event_")]
    basic_columns = ["event_id", "event_activity"]
    for col in columns:
        proj_df_0 = df[basic_columns + [col]].dropna()
        proj_df_0 = proj_df_0.groupby(col).first().reset_index()
        #proj_df = proj_df_0.groupby("event_id").first().reset_index()
        #proj_df_2 = proj_df_0.groupby("event_id").first().reset_index()
        proj_df = proj_df_0.copy()
        proj_df_2 = proj_df_0.copy()
        proj_df_2.columns = [x+"_2" for x in proj_df_2.columns]
        concat_df = pd.concat([proj_df, proj_df_2], axis=1)
        concat_df["event_activity_merge"] = concat_df["event_activity"] + "@@" + concat_df["event_activity_2"]
        #concat_df["event_id_2"] = concat_df["event_id"]
        ret[col] = concat_df

    return ret
