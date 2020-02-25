def get(dataframe, parameters=None):
    if parameters is None:
        parameters = {}

    r0 = dataframe["event_timestamp"].describe()

    ret = {x: str(y) for x, y in r0.items()}

    return ret
