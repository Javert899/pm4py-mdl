from pm4py.objects.log.exporter.parquet import factory as parquet_exporter

def apply(df, file_path, parameters=None):
    if parameters is None:
        parameters = {}

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        df.to_csv(file_path, index=False, sep=',', quotechar='\"')
    else:
        parquet_exporter.export_df(df, file_path, parameters=parameters)
