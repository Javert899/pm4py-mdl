from pm4pymdl.algo.mvp.gen_framework3.versions_discovery import classic

VARIANTS = {"classic": classic}


def apply(dataframe, parameters=None):
    if parameters is None:
        parameters = {}

    return VARIANTS["classic"].apply(dataframe, parameters=parameters)
