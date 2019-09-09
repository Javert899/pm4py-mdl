from pm4pymdl.objects.xoc.exporter.versions import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(df, dest_path, parameters=None, variant=CLASSIC):
    """
    Export into a XOC format

    Parameters
    ------------
    df
        Dataframe
    dest_path
        Destination path
    parameters
        Parameters of the algorithm
    variant
        Variant of the algorithm, possible values: classic
    """
    return VERSIONS[variant](df, dest_path, parameters=parameters)
