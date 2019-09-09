from pm4pymdl.objects.openslex.importer.versions import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(file_path, parameters=None, variant=CLASSIC):
    """
    Import an OpenSLEX model

    Parameters
    -------------
    file_path
        File path
    parameters
        Possible parameters of the algorithm
    variant
        Variant of the algorithm, possible values: class

    Returns
    -------------
    dataframe
        Dataframe
    """
    return VERSIONS[variant](file_path, parameters=parameters)
