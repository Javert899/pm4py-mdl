from pm4pymdl.algo.mvp.gen_framework3.versions_conformance import classic


VARIANTS = {"classic": classic}


def apply(dataframe, model, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].apply(dataframe, model, parameters=parameters)
