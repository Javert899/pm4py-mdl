import sqlite3
import os
import pandas as pd
from pm4py.objects.log.util import dataframe_utils


def apply(file_path, return_obj_dataframe=False, parameters=None):
    if parameters is None:
        parameters = {}

    db = sqlite3.connect(file_path)

    df = pd.read_sql_query("SELECT * FROM __EVENTS_TABLE", db)
    df = dataframe_utils.convert_timestamp_columns_in_df(df)

    if return_obj_dataframe:
        ot_df = pd.read_sql_query("SELECT * FROM __OBJECT_TYPES", db)

        OT = list(ot_df["NAME"])

        obj_df_list = []

        for ot in OT:
            o_df = pd.read_sql_query("SELECT * FROM " + ot, db)
            o_df = dataframe_utils.convert_timestamp_columns_in_df(o_df)

            obj_df_list.append(o_df)

        obj_df = pd.concat(obj_df_list)

        return df, obj_df

    return df

