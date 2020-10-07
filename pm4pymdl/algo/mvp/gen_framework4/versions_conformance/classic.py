from pm4pymdl.algo.mvp.gen_framework4.versions_discovery.classic import get_stream_from_dataframe, apply_stream


def apply(df, model, parameters=None):
    if parameters is None:
        parameters = {}

    stream = get_stream_from_dataframe(df, parameters=parameters)
    return apply_stream(stream, model, parameters=parameters)


def apply_stream(stream, model, parameters=None):
    log_model = apply_stream(stream, parameters=parameters)
