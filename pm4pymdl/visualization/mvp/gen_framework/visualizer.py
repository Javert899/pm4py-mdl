from pm4pymdl.visualization.mvp.gen_framework.versions import model1 as model1_visualization
from pm4pymdl.visualization.mvp.gen_framework.versions import model2 as model2_visualization
from pm4pymdl.visualization.mvp.gen_framework.versions import model3 as model3_visualization
from pm4pymdl.visualization.mvp.gen_framework.versions import procl_der_model

from pm4pymdl.algo.mvp.gen_framework.models import model_builder as model_factory

from graphviz import Digraph
from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
import uuid
import tempfile

def apply(model, parameters=None):
    if parameters is None:
        parameters = {}

    if model.type == model_factory.MODEL1:
        return model1_visualization.apply(model, parameters=parameters)
    elif model.type == model_factory.MODEL2:
        return model2_visualization.apply(model, parameters=parameters)
    elif model.type == model_factory.MODEL3:
        return model3_visualization.apply(model, parameters=parameters)
    elif model.type == "proclet_derivation_model":
        return procl_der_model.apply(model, parameters=parameters)

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
