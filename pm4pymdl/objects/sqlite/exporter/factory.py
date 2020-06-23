import sqlite3
import os
import traceback
import pandas as pd

def apply(df, file_path, obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    if os.path.exists(file_path):
        os.remove(file_path)

    cnx = sqlite3.connect(file_path)

    df = df.dropna(subset=["event_id"])
    df = df.dropna(subset=["event_activity"])
    df = df.dropna(subset=["event_timestamp"])
    df = df.dropna(how="all", axis=1)

    col = list(df.columns)
    rep_dict = {}
    for x in col:
        if x.startswith("event_"):
            rep_dict[x] = x.split("event_")[1]
        else:
            rep_dict[x] = "case_" + x
    df = df.rename(columns=rep_dict)

    df.to_sql(name="event_log", con=cnx, index=False)

    if obj_df is not None:
        OT = obj_df["object_type"].dropna().unique()
        dict = {"NAME": OT}
        ot_df = pd.DataFrame(dict)
        ot_df.to_sql(name="object_types", con=cnx, index=False)

        for ot in OT:
            red_df = obj_df[obj_df["object_type"] == ot].dropna(how="all", axis=1)

            red_df = red_df.dropna(subset=["object_id"]).drop(columns=["object_type"])

            col = list(red_df.columns)
            rep_dict = {}
            for x in col:
                rep_dict[x] = x.split("object_")[1]
            red_df = red_df.rename(columns=rep_dict)

            red_df.to_sql(name=ot, con=cnx, index=False)

    cnx.close()
