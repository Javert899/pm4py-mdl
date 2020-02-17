from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.algo.filtering.pandas.paths import paths_filter
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.util.constants import PARAMETER_CONSTANT_CASEID_KEY, PARAMETER_CONSTANT_ATTRIBUTE_KEY
import pandas as pd


def filter_paths(df, paths, parameters=None):
    """
    Apply a filter on traces containing / not containing a path

    Parameters
    ----------
    df
        Dataframe
    paths
        Paths to filter on
    parameters
        Possible parameters of the algorithm, including:
            case_id_glue -> Case ID column in the dataframe
            attribute_key -> Attribute we want to filter
            positive -> Specifies if the filter should be applied including traces (positive=True)
            or excluding traces (positive=False)
    Returns
    ----------
    df
        Filtered dataframe
    """
    try:
        if df.type == "succint":
            df = succint_mdl_to_exploded_mdl.apply(df)
    except:
        pass
    if parameters is None:
        parameters = {}
    paths = [path[0] + "," + path[1] for path in paths]
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    attribute_key = parameters[
        PARAMETER_CONSTANT_ATTRIBUTE_KEY] if PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY
    df = df.sort_values([case_id_glue, "event_timestamp"])
    positive = parameters["positive"] if "positive" in parameters else True
    filt_df = df[[case_id_glue, attribute_key, "event_id"]]
    filt_dif_shifted = filt_df.shift(-1)
    filt_dif_shifted.columns = [str(col) + '_2' for col in filt_dif_shifted.columns]
    stacked_df = pd.concat([filt_df, filt_dif_shifted], axis=1)
    stacked_df["@@path"] = stacked_df[attribute_key] + "," + stacked_df[attribute_key + "_2"]
    stacked_df = stacked_df[stacked_df["@@path"].isin(paths)]
    i1 = df.set_index("event_id").index
    i2 = stacked_df.set_index("event_id").index
    i3 = stacked_df.set_index("event_id_2").index
    if positive:
        return df[i1.isin(i2) | i1.isin(i3)]
    else:
        return df[~i1.isin(i2) & ~i1.isin(i3)]


def apply(df, min_freq=0):
    if min_freq > 0:
        persps = [x for x in df.columns if not x.startswith("event_")]
        collation = []
        for persp in persps:
            red_df = df.dropna(subset=[persp])
            prevlen = len(df)
            while True:
                dfg = df_statistics.get_dfg_graph(red_df, activity_key="event_activity", timestamp_key="event_timestamp", case_id_glue=persp)
                dfg = [x for x in dfg if dfg[x] >= min_freq]
                param = {}
                param[PARAMETER_CONSTANT_CASEID_KEY] = persp
                param[PARAMETER_CONSTANT_ATTRIBUTE_KEY] = "event_activity"
                red_df = filter_paths(red_df, dfg, parameters=param)
                thislen = len(red_df)
                dfg = df_statistics.get_dfg_graph(red_df, activity_key="event_activity", timestamp_key="event_timestamp", case_id_glue=persp)
                if len(dfg) == 0 or min(dfg.values()) >= min_freq or prevlen == thislen:
                    collation.append(red_df)
                    break
                prevlen = thislen
        return pd.concat(collation)
    return df
