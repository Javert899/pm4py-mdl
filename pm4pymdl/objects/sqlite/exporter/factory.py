from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl, succint_mdl_to_exploded_mdl
from copy import deepcopy
import pandas as pd
import sqlite3
import os


def apply(df, file_path, obj_df=None, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        os.remove(file_path)
    except:
        pass

    cnx = sqlite3.connect(file_path)

    df.to_sql(name="EVENT_TABLE", con=cnx)

    if obj_df is not None:
        OT = obj_df["object_type"].unique()
        for ot in OT:
            red_df = obj_df[obj_df["object_type"] == ot].dropna(how="all", axis=1)
            red_df.to_sql(name=ot, con=cnx)

    cnx.close()
