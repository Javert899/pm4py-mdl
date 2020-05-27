from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
from pm4pymdl.visualization.mvp.gen_framework2.versions import ptree, petri, dfg, ts


def apply(res, measure="frequency", freq="events", classifier="activity", projection="no", parameters=None):
    if parameters is None:
        parameters = {}

    if res["type"] == "ptree":
        return ptree.apply(res, measure=measure, freq=freq, classifier=classifier, projection=projection, parameters=parameters)
    elif res["type"] == "petri":
        return petri.apply(res, measure=measure, freq=freq, classifier=classifier, projection=projection, parameters=parameters)
    elif res["type"] == "dfg":
        return dfg.apply(res, measure=measure, freq=freq, classifier=classifier, projection=projection, parameters=parameters)
    elif res["type"] == "trans_system":
        return ts.apply(res, measure=measure, freq=freq, classifier=classifier, projection=projection, parameters=parameters)


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
