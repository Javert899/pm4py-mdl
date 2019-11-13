from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.objects.conversion.log import factory as log_conv_factory


def apply(df, persp, parameters=None):
    if df.type == "succint":
        df = succint_mdl_to_exploded_mdl(df)
        df.type = "exploded"

    cols = [x for x in df.columns if x.startswith("event_") or x == persp]
    df = df[cols].dropna(subset=[persp])

    renaming = {}
    renaming["event_activity"] = "concept:name"
    renaming["event_timestamp"] = "time:timestamp"
    renaming[persp] = "case:concept:name"

    df = df.rename(columns=renaming)

    log = log_conv_factory.apply(df)

    return log
