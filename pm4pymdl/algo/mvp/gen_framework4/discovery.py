from pm4pymdl.algo.mvp.gen_framework4.versions_discovery import classic

VARIANTS = {"classic": classic}


def apply(dataframe, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].apply(dataframe, parameters=parameters)


def apply_stream(stream, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].apply_stream(stream, parameters=parameters)


def get_stream_from_dataframe(df, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].get_stream_from_dataframe(df, parameters=parameters)
