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

    df.to_sql(name="__EVENTS_TABLE", con=cnx, index=False)

    if obj_df is not None:
        OT = obj_df["object_type"].unique()
        dict = {"NAME": OT}
        ot_df = pd.DataFrame(dict)
        ot_df.to_sql(name="__OBJECT_TYPES", con=cnx, index=False)

        for ot in OT:
            red_df = obj_df[obj_df["object_type"] == ot].dropna(how="all", axis=1)
            red_df.to_sql(name=ot, con=cnx, index=False)

    cnx.close()