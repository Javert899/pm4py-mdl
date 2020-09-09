from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
import tempfile
from graphviz import Digraph
import uuid

COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def apply(model, measure="frequency", freq="semantics", classifier="activity", projection="no", parameters=None):
    if parameters is None:
        parameters = {}

    model = model.dictio

    if freq == "events":
        prefix = "E="
    elif freq == "objects":
        prefix = "O="
    elif freq == "eo":
        prefix = "EO="
    else:
        prefix = ""
    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("pt", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})
    image_format = parameters["format"] if "format" in parameters else "png"

    min_act_freq = parameters["min_act_freq"] if "min_act_freq" in parameters else 0
    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 0

    types_keys = list(model["types_view"].keys())
    types_colors = {}

    for index, tk in enumerate(types_keys):
        type_color = COLORS[index % len(COLORS)]
        types_colors[tk] = type_color

    act_nodes = {}
    for act in model["activities"]:
        if not freq == "semantics":
            fr = model["activities"][act][freq]
        else:
            fr = model["activities"][act]["events"]
        if fr >= min_act_freq:
            if not freq == "semantics":
                label = act + "\n(" + prefix + str(fr) + ")"
            else:
                label = act
            this_uuid = str(uuid.uuid4())
            act_nodes[act] = this_uuid
            viz.node(this_uuid, label=label, shape="box")

    for index, tk in enumerate(types_keys):
        t = model["types_view"][tk]
        for edge in t["edges"]:
            source = edge[0]
            target = edge[1]
            if source in act_nodes and target in act_nodes:
                ann = t["edges"][edge][freq]
                if not freq == "semantics":
                    fr = t["edges"][edge][freq]
                else:
                    fr = t["edges"][edge]["events"]
                if fr >= min_edge_freq:
                    label = prefix + str(ann)
                    viz.edge(act_nodes[source], act_nodes[target], label=label, color=types_colors[tk])

        frk = "events" if freq == "semantics" else freq
        start_activities = [a for a in t["start_activities"] if
                            a in act_nodes and t["start_activities"][a][frk] >= min_edge_freq]
        end_activities = [a for a in t["end_activities"] if
                          a in act_nodes and t["end_activities"][a][frk] >= min_edge_freq]

        if start_activities:
            sa_uuid = str(uuid.uuid4())
            viz.node(sa_uuid, label="", shape="circle", style="filled", fillcolor=types_colors[tk])
            for sa in start_activities:
                sav = t["start_activities"][sa]
                label = prefix + str(sav[freq])
                viz.edge(sa_uuid, act_nodes[sa], label=label, color=types_colors[tk])

        if end_activities:
            ea_uuid = str(uuid.uuid4())
            viz.node(ea_uuid, label="", shape="circle", style="filled", fillcolor=types_colors[tk])
            for ea in end_activities:
                eav = t["end_activities"][ea]
                label = prefix + str(eav[freq])
                viz.edge(act_nodes[ea], ea_uuid, label=label, color=types_colors[tk])

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
