from pm4py.objects.log.exporter.parquet import factory as parquet_exporter
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl, succint_mdl_to_exploded_mdl
from copy import deepcopy

def apply(df, file_path, parameters=None):
    if parameters is None:
        parameters = {}

    if file_path.endswith(".csv") or file_path.endswith(".mdl"):
        conversion_needed = True
        try:
            if df.type == "succint":
                conversion_needed = False
        except:
            pass

        if conversion_needed:
            df = exploded_mdl_to_succint_mdl.apply(df)

        df.to_csv(file_path, index=False, sep=',', quotechar='\"')
    else:
        conversion_needed = False
        try:
            if df.type == "succint":
                conversion_needed = True
        except:
            pass

        if conversion_needed:
            df = succint_mdl_to_exploded_mdl.apply(df)
        new_parameters = deepcopy(parameters)
        new_parameters["compression"] = "gzip"
        parquet_exporter.export_df(df, file_path, parameters=new_parameters)
