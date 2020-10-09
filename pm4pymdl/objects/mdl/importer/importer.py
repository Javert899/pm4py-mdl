import pandas as pd

def apply(file_path, return_obj_dataframe=False, parameters=None):
    if parameters is None:
        parameters = {}

    sep = parameters["sep"] if "sep" in parameters else ","
    quotechar = parameters["quotechar"] if "quotechar" in parameters else "\""

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        all_df = pd.read_csv(file_path, sep=sep, quotechar=quotechar)
        eve_cols = [x for x in all_df.columns if not x.startswith("object_")]
        obj_cols = [x for x in all_df.columns if x.startswith("object_")]
        df = all_df[eve_cols]
        obj_df = pd.DataFrame()
        if obj_cols:
            obj_df = all_df[obj_cols]
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        if "event_start_timestamp" in df.columns:
            df["event_start_timestamp"] = pd.to_datetime(df["event_start_timestamp"])
        df = df.dropna(subset=["event_id"])
        df["event_id"] = df["event_id"].astype(str)
        df.type = "succint"
    elif file_path.endswith(".parquet"):
        from pm4pymdl.util.parquet_importer import importer as parquet_importer
        all_df = parquet_importer.apply(file_path)
        eve_cols = [x for x in all_df.columns if not x.startswith("object_")]
        obj_cols = [x for x in all_df.columns if x.startswith("object_")]
        df = all_df[eve_cols]
        df.type = "exploded"
        obj_df = pd.DataFrame()
        if obj_cols:
            obj_df = all_df[obj_cols]
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        df = df.dropna(subset=["event_id"])
        df.type = "exploded"

    if return_obj_dataframe:
        obj_df = obj_df.dropna(subset=["object_id"])
        return df, obj_df

    return df
