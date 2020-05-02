import random
import time
from copy import copy

from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.algo.filtering.pandas.attributes import attributes_filter
from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
from pm4py.objects.heuristics_net.net import HeuristicsNet
from pm4py.util import constants
import pandas as pd
from collections import Counter

DEPENDENCY_THRESH = "dependency_thresh"
AND_MEASURE_THRESH = "and_measure_thresh"
MIN_ACT_COUNT = "min_act_count"
MIN_DFG_OCCURRENCES = "min_dfg_occurrences"
DFG_PRE_CLEANING_NOISE_THRESH = "dfg_pre_cleaning_noise_thresh"
LOOP_LENGTH_TWO_THRESH = "loop_length_two_thresh"

DEFAULT_DEPENDENCY_THRESH = -0.9
DEFAULT_AND_MEASURE_THRESH = 0.65
DEFAULT_MIN_ACT_COUNT = 1
DEFAULT_MIN_DFG_OCCURRENCES = 1
DEFAULT_DFG_PRE_CLEANING_NOISE_THRESH = -0.05
DEFAULT_LOOP_LENGTH_TWO_THRESH = 0.5

DECREASING_FACTOR = "decreasingFactor"
PERFORMANCE = "performance"
PERSPECTIVES = "perspectives"
USE_TIMESTAMP = "use_timestamp"
SORT_CASEID_REQUIRED = "sort_caseid_required"
SORT_TIMESTAMP_REQUIRED = "sort_timestamp_required"

COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def clean_sa_ea(dictio, decreasing_factor):
    """
    Clean start and end activities by using decreasing factor

    Parameters
    -------------
    dictio
        Dictionary of start and end activities
    decreasing_factor
        Decreasing factor

    Returns
    -------------
    cleaned_dictio
        Cleaned dictionary
    """
    cleaned_dictio = {}
    ordered_list = sorted([(x, y) for x, y in dictio.items()], key=lambda x: x[1], reverse=True)
    i = 0
    while i < len(ordered_list):
        if i == 0:
            cleaned_dictio[ordered_list[i][0]] = ordered_list[i][1]
        else:
            ratio = ordered_list[i][1] / ordered_list[i - 1][1]
            if ratio >= decreasing_factor:
                cleaned_dictio[ordered_list[i][0]] = ordered_list[i][1]
            else:
                break
        i = i + 1
    return cleaned_dictio


def apply(df, parameters=None):
    """
    Discover a StarStar model from an ad-hoc built dataframe

    Parameters
    -------------
    df
        Dataframe
    parameters
        Possible parameters of the algorithm

    Returns
    -------------
    perspectives_heu
        Dictionary of perspectives associated to Heuristics Net
    """

    if parameters is None:
        parameters = {}

    if len(df) == 0:
        df = pd.DataFrame({"event_id": [], "event_activity": []})

    dependency_thresh = parameters[
        DEPENDENCY_THRESH] if DEPENDENCY_THRESH in parameters else DEFAULT_DEPENDENCY_THRESH
    and_measure_thresh = parameters[
        AND_MEASURE_THRESH] if AND_MEASURE_THRESH in parameters else DEFAULT_AND_MEASURE_THRESH
    min_act_count = parameters[MIN_ACT_COUNT] if MIN_ACT_COUNT in parameters else DEFAULT_MIN_ACT_COUNT
    min_dfg_occurrences = parameters[
        MIN_DFG_OCCURRENCES] if MIN_DFG_OCCURRENCES in parameters else DEFAULT_MIN_DFG_OCCURRENCES
    dfg_pre_cleaning_noise_thresh = parameters[
        DFG_PRE_CLEANING_NOISE_THRESH] if DFG_PRE_CLEANING_NOISE_THRESH in parameters else DEFAULT_DFG_PRE_CLEANING_NOISE_THRESH
    decreasing_factor_sa_ea = parameters[DECREASING_FACTOR] if DECREASING_FACTOR in parameters else 0.5
    performance = parameters[PERFORMANCE] if PERFORMANCE in parameters else False
    perspectives = parameters[PERSPECTIVES] if PERSPECTIVES in parameters else None
    use_timestamp = parameters[USE_TIMESTAMP] if USE_TIMESTAMP in parameters else True
    sort_caseid_required = parameters[SORT_CASEID_REQUIRED] if SORT_CASEID_REQUIRED in parameters else True
    sort_timestamp_required = parameters[SORT_TIMESTAMP_REQUIRED] if SORT_TIMESTAMP_REQUIRED in parameters else True

    perspectives_heu = {}

    r = lambda: random.randint(0, 255)
    if perspectives is None:
        perspectives = list(x for x in df.columns if not x.startswith("event"))

        # del perspectives[perspectives.index("event_id")]
        # del perspectives[perspectives.index("event_activity")]
        # if "event_timestamp" in perspectives:
        #    del perspectives[perspectives.index("event_timestamp")]
        perspectives = sorted(perspectives)
    activities_occurrences = attributes_filter.get_attribute_values(df.groupby("event_id").first().reset_index(),
                                                                    "event_activity")
    activities_occurrences = {x: y for x, y in activities_occurrences.items() if y >= min_act_count}
    for p_ind, p in enumerate(perspectives):
        has_timestamp = False
        if "event_timestamp" in df.columns and use_timestamp:
            proj_df = df[["event_id", "event_activity", "event_timestamp", p]].dropna(subset=[p])
            has_timestamp = True
        else:
            proj_df = df[["event_id", "event_activity", p]].dropna(subset=[p])

        """
        proj_df_activities = Counter(list(proj_df["event_activity"]))
        proj_df_activities = set(x for x in proj_df_activities if proj_df_activities[x] >= min_act_count)
        proj_df = proj_df[proj_df["event_activity"].isin(proj_df_activities)]
        print(proj_df_activities)"""

        # proj_df = proj_df.groupby(["event_id", "event_activity", p]).first().reset_index()
        # print('sii')

        proj_df = proj_df[proj_df["event_activity"].isin(activities_occurrences)]
        proj_df_first = proj_df.groupby("event_id").first().reset_index()

        if performance:
            dfg_frequency, dfg_performance = df_statistics.get_dfg_graph(proj_df, activity_key="event_activity",
                                                                         case_id_glue=p,
                                                                         timestamp_key="event_timestamp",
                                                                         measure="both",
                                                                         sort_caseid_required=sort_caseid_required,
                                                                         sort_timestamp_along_case_id=sort_timestamp_required)
        else:
            if has_timestamp:
                dfg_frequency = df_statistics.get_dfg_graph(proj_df, activity_key="event_activity", case_id_glue=p,
                                                            timestamp_key="event_timestamp",
                                                            sort_caseid_required=sort_caseid_required,
                                                            sort_timestamp_along_case_id=sort_timestamp_required)
            else:
                dfg_frequency = df_statistics.get_dfg_graph(proj_df, activity_key="event_activity", case_id_glue=p,
                                                            sort_timestamp_along_case_id=False,
                                                            sort_caseid_required=sort_caseid_required)
        if len(dfg_frequency) > 0:
            this_color = COLORS[p_ind] if p_ind < len(COLORS) else '#%02X%02X%02X' % (r(), r(), r())
            parameters_sa_ea = copy(parameters)
            parameters_sa_ea[constants.PARAMETER_CONSTANT_CASEID_KEY] = p
            parameters_sa_ea[constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY] = "event_activity"
            parameters_sa_ea[constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = "event_activity"
            start_activities = start_activities_filter.get_start_activities(proj_df, parameters=parameters_sa_ea)
            end_activities = end_activities_filter.get_end_activities(proj_df, parameters=parameters_sa_ea)
            start_activities = clean_sa_ea(start_activities, decreasing_factor_sa_ea)
            end_activities = clean_sa_ea(end_activities, decreasing_factor_sa_ea)
            max_entry = dict()
            max_exit = dict()
            for x in dfg_frequency:
                a1 = x[0]
                a2 = x[1]
                y = dfg_frequency[x]
                if not a1 == a2:
                    if not a2 in max_entry or max_entry[a2][1] < y:
                        max_entry[a2] = [x, y]
                    if not a1 in max_exit or max_exit[a1][1] < y:
                        max_exit[a1] = [x, y]
            max_entry = set(y[0] for y in max_entry.values())
            max_exit = set(y[0] for y in max_exit.values())
            #max_entry = {}
            #max_exit = {}
            dfg_frequency = {x: y for x, y in dfg_frequency.items() if
                             y >= min_dfg_occurrences or x in max_entry or x in max_exit}
            activities = list(activities_occurrences.keys())

            if performance:
                dfg_performance = {x: y for x, y in dfg_performance.items() if x in dfg_frequency}
                heu_net = HeuristicsNet(dfg_frequency, start_activities=start_activities, end_activities=end_activities,
                                        default_edges_color=this_color, net_name=p, activities=activities,
                                        activities_occurrences=activities_occurrences, performance_dfg=dfg_performance)
            else:
                heu_net = HeuristicsNet(dfg_frequency, start_activities=start_activities, end_activities=end_activities,
                                        default_edges_color=this_color, net_name=p, activities=activities,
                                        activities_occurrences=activities_occurrences)
            heu_net.calculate(dependency_thresh=dependency_thresh, and_measure_thresh=and_measure_thresh,
                              min_act_count=1, min_dfg_occurrences=1,
                              dfg_pre_cleaning_noise_thresh=dfg_pre_cleaning_noise_thresh)
            if len(heu_net.nodes) > 0:
                perspectives_heu[p] = heu_net

    return perspectives_heu
