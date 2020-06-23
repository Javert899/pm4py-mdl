import sqlite3
import os
import pandas as pd
from pm4py.objects.log.util import dataframe_utils


def apply(file_path, return_obj_dataframe=False, parameters=None):
    if parameters is None:
        parameters = {}

    db = sqlite3.connect(file_path)

    df = pd.read_sql_query("SELECT * FROM event_log", db)

    col = list(df.columns)
    rep_dict = {}
    for x in col:
        if x.startswith("case_"):
            rep_dict[x] = x.split("case_")[1]
        else:
            rep_dict[x] = "event_" + x
    df = df.rename(columns=rep_dict)

    df = df.dropna(subset=["event_id"])
    df = df.dropna(subset=["event_activity"])
    df = df.dropna(subset=["event_timestamp"])

    df = dataframe_utils.convert_timestamp_columns_in_df(df)

    """print(df)

    ot_columns = [x for x in df.columns if not x.startswith("event_")]
    for ot in ot_columns:
        df[ot] = df[ot].apply(lambda x: eval(x))

    print(df)"""

    if return_obj_dataframe:
        ot_df = pd.read_sql_query("SELECT * FROM object_types", db)
        ot_df = ot_df.dropna(subset=["NAME"])

        OT = list(ot_df["NAME"])

        obj_df_list = []

        for ot in OT:
            o_df = pd.read_sql_query("SELECT * FROM " + ot, db)

            col = list(o_df.columns)
            rep_dict = {}
            for x in col:
                rep_dict[x] = "object_"+x
            o_df = o_df.rename(columns=rep_dict)

            o_df = o_df.dropna(subset=["object_id"])
            o_df["object_type"] = ot

            o_df = dataframe_utils.convert_timestamp_columns_in_df(o_df)

            obj_df_list.append(o_df)

        obj_df = pd.concat(obj_df_list)

        return df, obj_df

    return df

