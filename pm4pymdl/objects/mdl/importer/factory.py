import pandas as pd

def apply(file_path, parameters=None):
    if parameters is None:
        parameters = {}

    sep = parameters["sep"] if "sep" in parameters else ","
    quotechar = parameters["quotechar"] if "quotechar" in parameters else "\""

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        df = pd.read_csv(file_path, sep=sep, quotechar=quotechar)
        df.type = "succint"
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        return df
    elif file_path.endswith(".parquet"):
        from pm4py.objects.log.importer.parquet import factory as parquet_importer
        df = parquet_importer.apply(file_path)
        df.type = "exploded"
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        return df
