from pm4py.algo.discovery.dfg.utils.dfg_utils import get_max_activity_count, get_activities_from_dfg
from pm4py.algo.filtering.common import filtering_constants
from pm4py.util import constants

def clean_dfg_based_on_noise_thresh(dfg, activities, noise_threshold, parameters=None):
    """
    Clean Directly-Follows graph based on noise threshold

    Parameters
    ----------
    dfg
        Directly-Follows graph
    activities
        Activities in the DFG graph
    noise_threshold
        Noise threshold

    Returns
    ----------
    newDfg
        Cleaned dfg based on noise threshold
    """
    if parameters is None:
        parameters = {}

    most_common_paths = parameters[
        constants.PARAM_MOST_COMMON_PATHS] if constants.PARAM_MOST_COMMON_PATHS in parameters else None
    if most_common_paths is None:
        most_common_paths = []

    new_dfg = None

    for el in dfg:
        if new_dfg is None:
            new_dfg = {}
        act1 = el.split("@@")[0]
        act2 = el.split("@@")[1]
        val = dfg[el]

        if not el in most_common_paths and val < min(activities[act1] * noise_threshold,
                                                     activities[act2] * noise_threshold):
            pass
        else:
            new_dfg[el] = dfg[el]

    if new_dfg is None:
        return dfg

    return new_dfg
