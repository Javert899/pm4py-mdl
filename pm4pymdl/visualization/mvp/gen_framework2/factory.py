from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
from pm4pymdl.visualization.mvp.gen_framework2.versions import ptree


def apply(res, parameters=None):
    if parameters is None:
        parameters = {}

    if res["type"] == "ptree":
        return ptree.apply(res, parameters=parameters)


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
