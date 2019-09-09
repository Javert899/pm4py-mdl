from pm4pymdl.objects.xoc.importer.versions import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(file_path, parameters=None, variant=CLASSIC):
    """
    Apply the importing of a XOC file

    Parameters
    ------------
    file_path
        Path to the XOC file
    parameters
        Import parameters
    variant
        Variant of the algorithm, including: classic

    Returns
    ------------
    dataframe
        Dataframe
    """
    return VERSIONS[variant](file_path, parameters=parameters)
