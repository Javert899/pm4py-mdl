from pm4pymdl.visualization.petrinet.versions import from_log_and_replay, dfg_mining
from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave


FROM_LOG_AND_REPLAY = "from_log_and_replay"
DFG_MINING = "dfg_mining"

VERSIONS = {FROM_LOG_AND_REPLAY: from_log_and_replay.apply, DFG_MINING: dfg_mining.apply}


def apply(obj, variant=FROM_LOG_AND_REPLAY, parameters=None):
    return VERSIONS[variant](obj, parameters=parameters)


def save(gviz, output_file_path):
    """
    Save the diagram

    Parameters
    -----------
    gviz
        GraphViz diagram
    output_file_path
        Path where the GraphViz output should be saved
    """
    gsave.save(gviz, output_file_path)


def view(gviz):
    """
    View the diagram

    Parameters
    -----------
    gviz
        GraphViz diagram
    """
    return gview.view(gviz)
