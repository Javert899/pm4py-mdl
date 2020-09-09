from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
import tempfile
from graphviz import Digraph
import uuid

def apply(model, measure="frequency", freq="events", classifier="activity", projection="no", parameters=None):
    if parameters is None:
        parameters = {}

    model = model.dictio

    if freq == "events":
        prefix = "E="
    elif freq == "objects":
        prefix = "O="
    elif freq == "eo":
        prefix = "EO="
    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("pt", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})
    image_format = parameters["format"] if "format" in parameters else "png"

    min_act_freq = parameters["min_act_freq"] if "min_act_freq" in parameters else 0
    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 0

    act_nodes = {}
    for act in model["activities"]:
        ann = model["activities"][act][freq]
        if ann >= min_act_freq:
            label = act + "\n(" + prefix + str(ann) + ")"
            this_uuid = str(uuid.uuid4())
            act_nodes[act] = this_uuid
            viz.node(this_uuid, label=label, shape="box")

    for tk in model["types_view"]:
        t = model["types_view"][tk]
        for edge in t["edges"]:
            source = edge[0]
            target = edge[1]
            if source in act_nodes and target in act_nodes:
                ann = t["edges"][edge][freq]
                if ann >= min_edge_freq:
                    label = prefix + str(ann)
                    viz.edge(act_nodes[source], act_nodes[target], label=label)

    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz


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
