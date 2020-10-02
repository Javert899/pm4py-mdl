import pandas as pd
import os
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter
from pm4pymdl.algo.mvp.utils.exploded_mdl_to_succint_mdl import apply

logs = os.listdir("pkdd99")
case_id_columns = ["account_id", "client_id", "disp_id", "order_id", "trans_id", "loan_id", "card_id", "district_id"]


def do_column_mapping(df):
    columns = list(df.columns)
    renaming = {}
    for i in range(len(columns)):
        if columns[i] == "date":
            renaming[columns[i]] = "event_timestamp"
        elif columns[i] == "issued":
            renaming[columns[i]] = "event_timestamp"
        elif columns[i] == "birth_number":
            renaming[columns[i]] = "event_timestamp"
        elif columns[i] not in case_id_columns:
            renaming[columns[i]] = "event_" + columns[i]
        else:
            renaming[columns[i]] = columns[i]
    df = df.rename(columns=renaming)
    return df

def set_up_activity(l0, df):
    if "account" in l0:
        print("yeees")
        df["event_activity"] = "account frequency="+df["event_frequency"]
    elif "client" in l0:
        df["event_activity"] = "client"
    elif "disp" in l0:
        df["event_activity"] = "disposition type="+df["event_type"]
    elif "order" in l0:
        df["event_activity"] = "order k_symbol="+df["event_k_symbol"]
    elif "trans" in l0:
        df["event_activity"] = "trans type="+df["event_type"]+" operation="+df["event_operation"]+" k_symbol="+df["event_k_symbol"]
    elif "loan" in l0:
        df["event_activity"] = "loan status="+df["event_status"]
    elif "card" in l0:
        df["event_activity"] = "card type="+df["event_type"]
    else:
        print("l0",l0,"not found")
    return df


if __name__ == "__main__":
    all_df = []
    for l0 in logs:
        df = pd.read_csv("pkdd99/"+l0, sep=";", quotechar="\"")
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], format="%y%m%d")
        if "issued" in df.columns:
            df["issued"] = pd.to_datetime(df["issued"], format="%y%m%d %H:%M:%S")
        if "birth_number" in df.columns:
            df["birth_number"] = 1900 + df["birth_number"] // 10000
            df["birth_number"] = pd.to_datetime(df["birth_number"], format="%Y", errors="ignore")
        for column in case_id_columns:
            if column in df.columns:
                df[column] = df[column].astype(str)
        df = do_column_mapping(df)
        df = set_up_activity(l0, df)
        if "event_activity" in df.columns and "event_timestamp" in df.columns:
            df = df.dropna(subset=["event_timestamp", "event_activity"], how="any")
            if len(df) > 0:
                    all_df.append(df)

    df = pd.concat(all_df)
    df = df.reset_index()
    df["event_id"] = df.index
    df["event_id"] = df["event_id"].astype(str)
    df = df.sort_values(["event_timestamp", "event_id"])
    df = df.reset_index()
    df.type = "exploded"
    del df["index"]
    del df["level_0"]
    print(df.columns)
    mdl_exporter.apply(df, "pkdd99.parquet")
