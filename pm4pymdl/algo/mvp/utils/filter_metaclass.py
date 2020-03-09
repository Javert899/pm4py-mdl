from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
import pandas as pd


def do_filtering(dataframe, fd0, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        if dataframe.type == "succint":
            dataframe = succint_mdl_to_exploded_mdl.apply(dataframe)
    except:
        pass

    fd1 = dataframe[dataframe["event_id"].isin(fd0["event_id"])]
    cols = [x for x in fd1.columns if not x.startswith("event_")]
    cols_values = {}
    collation = []
    for c in cols:
        cols_values[c] = list(fd1.dropna(subset=[c])[c].unique())
        collation.append(dataframe[dataframe[c].isin(cols_values[c])])

    if collation:
        df2 = pd.concat(collation)
    else:
        df2 = pd.DataFrame()

    return df2


def do_negative_filtering(dataframe, fd0, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        if dataframe.type == "succint":
            dataframe = succint_mdl_to_exploded_mdl.apply(dataframe)
    except:
        pass

    fd1 = dataframe[dataframe["event_id"].isin(fd0["event_id"])]
    cols = [x for x in fd1.columns if not x.startswith("event_")]
    cols_values = {}
    collation = []
    for c in cols:
        cols_values[c] = list(fd1.dropna(subset=[c])[c].unique())
        collation.append(dataframe[dataframe[c].isin(cols_values[c])])

    if collation:
        df2 = pd.concat(collation)
    else:
        df2 = pd.DataFrame()

    i1 = dataframe.index
    i2 = df2.index

    return dataframe[~i1.isin(i2)]
