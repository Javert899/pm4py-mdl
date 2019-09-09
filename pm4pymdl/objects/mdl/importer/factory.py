from pm4py.objects.log.importer.parquet import factory as parquet_importer
import pandas as pd

def apply(file_path, parameters=None):
    if parameters is None:
        parameters = {}

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        df = pd.read_csv(file_path, sep=',', quotechar='\"')
        df.type = "succint"
        return df
    elif file_path.endswith(".parquet"):
        df = parquet_importer.apply(file_path)
        df.type = "succint"
        return df
