import pandas as pd


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    perspectives = [x for x in df.columns if not x.startswith("event_")]

    ret = {}
    basic_columns = ["event_id", "event_activity"]

    for c in perspectives:
        proj_df = df[basic_columns + [c]].dropna()
        proj_df_2 = proj_df.copy()

        merged_df = proj_df.merge(proj_df_2, how='inner', left_on=[c, "event_activity"], right_on=[c, "event_activity"], suffixes=('','_2'))
        #merged_df = merged_df[merged_df["event_id"] == merged_df["event_id_2"]]
        merged_df["@@eid_c_concat"] = merged_df["event_id"] + "@@@" + merged_df[c]

        g_m_df = merged_df["@@eid_c_concat"].groupby(merged_df["@@eid_c_concat"]).transform('size')

        merged_df = merged_df[g_m_df == 1]

        merged_df["event_activity_2"] = merged_df["event_activity"]
        merged_df["event_activity_merge"] = merged_df["event_activity"] + "@@" + merged_df["event_activity_2"]
        merged_df = merged_df.reset_index()

        ret[c] = merged_df

    return ret
