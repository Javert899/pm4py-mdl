from pm4py.objects.log.importer.parquet import factory as parquet_importer
import pandas as pd

def apply(file_path, parameters=None):
    if parameters is None:
        parameters = {}

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        df = pd.read_csv(file_path, sep=',', quotechar='\"')
        return df
    elif file_path.endswith(".parquet"):
        return parquet_importer.apply(file_path)
