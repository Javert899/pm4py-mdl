from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.objects.conversion.log import converter


def apply(df, parameters=None):
    if parameters is None:
        parameters = {}

    stream = get_stream_from_dataframe(df, parameters=parameters)

    return apply_stream(stream, parameters=parameters)


def get_stream_from_dataframe(df, parameters=None):
    if parameters is None:
        parameters = {}

    df_type = df.type
    df = df.sort_values(["event_timestamp", "event_id"])
    if df_type == "succint":
        df = succint_mdl_to_exploded_mdl.apply(df)

    columns = [x for x in df.columns if not x.startswith("event") or x == "event_activity" or x == "event_id" or x == "event_timestamp"]
    df = df[columns]

    stream = converter.apply(df, variant=converter.Variants.TO_EVENT_STREAM)

    return stream


def apply_stream(stream, parameters=None):
    if parameters is None:
        parameters = {}

    support = parameters["support"] if "support" in parameters else 1
    epsilon = parameters["epsilon"] if "epsilon" in parameters else 0.0
    debug = parameters["debug"] if "debug" in parameters else False
