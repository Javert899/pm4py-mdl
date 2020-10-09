from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
import tempfile
from graphviz import Digraph
import uuid

COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]

def human_readable_stat(c):
    """
    Transform a timedelta expressed in seconds into a human readable string

    Parameters
    ----------
    c
        Timedelta expressed in seconds

    Returns
    ----------
    string
        Human readable string
    """
    c = int(float(c))
    years = c // 31104000
    months = c // 2592000
    days = c // 86400
    hours = c // 3600 % 24
    minutes = c // 60 % 60
    seconds = c % 60
    if years > 0:
        return str(years) + "Y"
    if months > 0:
        return str(months) + "MO"
    if days > 0:
        return str(days) + "D"
    if hours > 0:
        return str(hours) + "h"
    if minutes > 0:
        return str(minutes) + "m"
    return str(seconds) + "s"


def apply(model, measure="frequency", freq="events", classifier="activity", projection="no", count_events=False, parameters=None):
    if parameters is None:
        parameters = {}

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

    types_keys = sorted(list(model["types_view"].keys()))
    types_colors = {}

    FONTSIZE_NODES = '26'

    if freq == "semantics":
        FONTSIZE_EDGES = '19'
    else:
        FONTSIZE_EDGES = '26'


    FONTNAME_NODES = 'bold'
    FONTNAME_EDGES = 'bold'

    PENWIDTH = '4'

    for index, tk in enumerate(types_keys):
        type_color = COLORS[index % len(COLORS)]
        types_colors[tk] = type_color

    act_nodes = {}
    ordered_activities = sorted(model["activities"])
    for act in ordered_activities:
        fr_ev = model["activities"][act]["events"]
        if not freq == "semantics":
            fr = model["activities"][act][freq]
        else:
            fr = fr_ev

        if count_events:
            fr_incl = fr_ev
        else:
            fr_incl = fr

        if fr_incl >= min_act_freq:
            map_act = model["activities_mapping"][act]
            act_type_color = types_colors[map_act]
            if not freq == "semantics":
                label = act + "\n" + prefix + str(fr) + ""
            else:
                label = act
            this_uuid = str(uuid.uuid4())
            act_nodes[act] = this_uuid
            viz.node(this_uuid, label=label, color=act_type_color, shape="box", fontsize=FONTSIZE_NODES, fontname=FONTNAME_NODES)

    for index, tk in enumerate(types_keys):
        t = model["types_view"][tk]
        edges = sorted(list(t["edges"]))
        for edge in edges:
            source = edge[0]
            target = edge[1]
            source_type = model["activities_mapping"][source]
            target_type = model["activities_mapping"][target]

            if source in act_nodes and target in act_nodes:
                if projection == "no" or (projection == "source" and source_type == tk) or (projection == "target" and target_type == tk):
                    ann = t["edges"][edge][freq]
                    style = "dashed"

                    this_penwidth = PENWIDTH
                    freq_ev = t["edges"][edge]["events"]
                    if not freq == "semantics":
                        fr = t["edges"][edge][freq]
                    else:
                        fr = freq_ev

                    if count_events:
                        fr_incl = freq_ev
                    else:
                        fr_incl = fr

                    if fr_incl >= min_edge_freq:

                        if t["edges"][edge]["must_exit"]:
                            arrowtail = "box"
                        else:
                            arrowtail = None

                        if t["edges"][edge]["must_entry"]:
                            arrowhead = "normal"
                        else:
                            arrowhead = "curve"

                        if arrowtail == "box" and arrowhead == "normal":
                            style = "solid"

                        if measure == "frequency":
                            label = prefix + str(ann)
                        else:
                            if freq == "events":
                                label = "E=%s" % (human_readable_stat(t["edges"][edge]["performance_events"]))
                            elif freq == "semantics":
                                label = ""
                            else:
                                label = "EO=%s" % (human_readable_stat(t["edges"][edge]["performance_eo"]))
                        if arrowtail is not None:
                            viz.edge(act_nodes[source], act_nodes[target], style=style, label=label, color=types_colors[tk],
                                     fontcolor=types_colors[tk], fontsize=FONTSIZE_EDGES, fontname=FONTNAME_EDGES,
                                     penwidth=this_penwidth, arrowhead=arrowhead, arrowtail=arrowtail, dir="both")
                        else:
                            viz.edge(act_nodes[source], act_nodes[target], style=style, label=label, color=types_colors[tk],
                                     fontcolor=types_colors[tk], fontsize=FONTSIZE_EDGES, fontname=FONTNAME_EDGES,
                                     penwidth=this_penwidth, arrowhead=arrowhead)
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
