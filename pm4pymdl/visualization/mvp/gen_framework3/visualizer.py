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
    viz.attr(overlap='false')
    #viz.attr(fontsize='26')
    image_format = parameters["format"] if "format" in parameters else "png"

    min_act_freq = parameters["min_act_freq"] if "min_act_freq" in parameters else 0
    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 0

    types_keys = list(model["types_view"].keys())
    types_colors = {}

    FONTSIZE_NODES = '26'
    FONTSIZE_EDGES = '26'

    FONTNAME_NODES = 'bold'
    FONTNAME_EDGES = 'bold'

    SOLID_PENWIDTH = '8'
    DASHED_PENWIDTH = '2'

    for index, tk in enumerate(types_keys):
        type_color = COLORS[index % len(COLORS)]
        types_colors[tk] = type_color

    act_nodes = {}
    for act in model["activities"]:
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
            act_type_color = types_colors[map_act[0]]
            if not freq == "semantics":
                label = act + "\n" + prefix + str(fr) + ""
            else:
                label = act
            this_uuid = str(uuid.uuid4())
            act_nodes[act] = this_uuid
            if map_act[1] == model["activities"][act]["events"]:
                viz.node(this_uuid, label=label, fillcolor=act_type_color, shape="box", style="filled", fontsize=FONTSIZE_NODES, fontname=FONTNAME_NODES)
            else:
                viz.node(this_uuid, label=label, color=act_type_color, shape="box", fontsize=FONTSIZE_NODES, fontname=FONTNAME_NODES)

    for index, tk in enumerate(types_keys):
        t = model["types_view"][tk]
        for edge in t["edges"]:
            source = edge[0]
            target = edge[1]
            source_type = model["activities_mapping"][source][0]
            target_type = model["activities_mapping"][target][0]

            if source in act_nodes and target in act_nodes:
                if projection == "no" or (projection == "source" and source_type == tk) or (projection == "target" and target_type == tk):
                    ann = t["edges"][edge][freq]
                    if t["edges"][edge]["must"]:
                        style = "solid"
                        this_penwidth = SOLID_PENWIDTH
                    else:
                        style = "dashed"
                        this_penwidth = DASHED_PENWIDTH
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
                        if measure == "frequency":
                            label = prefix + str(ann)
                        else:
                            if freq == "events":
                                label = "E=%s" % (human_readable_stat(t["edges"][edge]["performance_events"]))
                            elif freq == "semantics":
                                label = ""
                            else:
                                label = "EO=%s" % (human_readable_stat(t["edges"][edge]["performance_eo"]))
                        viz.edge(act_nodes[source], act_nodes[target], style=style, label=label, color=types_colors[tk], fontcolor=types_colors[tk], fontsize=FONTSIZE_EDGES, fontname=FONTNAME_EDGES, penwidth=this_penwidth)

        if count_events:
            frk = "events"
        else:
            frk = "events" if freq == "semantics" else freq

        start_activities = [a for a in t["start_activities"] if
                            a in act_nodes and t["start_activities"][a][frk] >= min_edge_freq]
        end_activities = [a for a in t["end_activities"] if
                          a in act_nodes and t["end_activities"][a][frk] >= min_edge_freq]

        if start_activities:
            sa_uuid = str(uuid.uuid4())
            viz.node(sa_uuid, label=tk, shape="underline", fillcolor=types_colors[tk], color=types_colors[tk], fontcolor=types_colors[tk], fontsize=FONTSIZE_NODES, fontname=FONTNAME_NODES)
            for sa in start_activities:
                sav = t["start_activities"][sa]
                if measure == "frequency":
                    label = prefix + str(sav[freq])
                else:
                    label = ""

                if sav["must"]:
                    style = "solid"
                    this_penwidth = SOLID_PENWIDTH
                else:
                    style = "dashed"
                    this_penwidth = DASHED_PENWIDTH
                viz.edge(sa_uuid, act_nodes[sa], style=style, label=label, color=types_colors[tk], fontcolor=types_colors[tk], fontsize=FONTSIZE_EDGES, fontname=FONTNAME_EDGES, penwidth=this_penwidth)

        if end_activities:
            ea_uuid = str(uuid.uuid4())
            viz.node(ea_uuid, label=tk, shape="none", fillcolor=types_colors[tk], color=types_colors[tk], fontcolor=types_colors[tk], fontsize=FONTSIZE_NODES, fontname=FONTNAME_NODES)
            for ea in end_activities:
                eav = t["end_activities"][ea]
                if measure == "frequency":
                    label = prefix + str(eav[freq])
                else:
                    label = ""

                if eav["must"]:
                    style = "solid"
                    this_penwidth = SOLID_PENWIDTH
                else:
                    style = "dashed"
                    this_penwidth = DASHED_PENWIDTH
                viz.edge(act_nodes[ea], ea_uuid, style=style, label=label, color=types_colors[tk], fontcolor=types_colors[tk], fontsize=FONTSIZE_EDGES, fontname=FONTNAME_EDGES, penwidth=this_penwidth)

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
