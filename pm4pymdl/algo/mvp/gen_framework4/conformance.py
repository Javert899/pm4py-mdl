from pm4pymdl.algo.mvp.gen_framework4.versions_conformance import classic

VARIANTS = {"classic": classic}


def apply(dataframe, model, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].apply(dataframe, model, parameters=parameters)


def apply_stream(stream, model, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].apply_stream(stream, model, parameters=parameters)
