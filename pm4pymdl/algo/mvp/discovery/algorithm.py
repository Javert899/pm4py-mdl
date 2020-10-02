from pm4pymdl.algo.mvp.discovery.versions import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(dataframe, parameters=None, variant=CLASSIC):
    """
    Discover a StarStar model from an ad-hoc built dataframe

    Parameters
    -------------
    df
        Dataframe
    parameters
        Possible parameters of the algorithm
    variant
        Variant of the algorithm, possible values: classic

    Returns
    -------------
    perspectives_heu
        Dictionary of perspectives associated to Heuristics Net
    """
    return VERSIONS[variant](dataframe, parameters=parameters)
