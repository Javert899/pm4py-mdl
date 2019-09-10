from pm4py.objects.log.exporter.parquet import factory as parquet_exporter
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl
from copy import deepcopy

def apply(df, file_path, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        if df.type == "exploded":
               df = exploded_mdl_to_succint_mdl.apply(df)
    except:
        pass

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        df.to_csv(file_path, index=False, sep=',', quotechar='\"')
    else:
        new_parameters = deepcopy(parameters)
        new_parameters["compression"] = "gzip"
        parquet_exporter.export_df(df, file_path, parameters=new_parameters)
