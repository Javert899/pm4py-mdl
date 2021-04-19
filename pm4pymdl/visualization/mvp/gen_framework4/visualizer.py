from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
import tempfile
from graphviz import Digraph
import uuid
import math


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


def get_corr_hex(num):
    """
    Gets correspondence between a number
    and an hexadecimal string

    Parameters
    -------------
    num
        Number

    Returns
    -------------
    hex_string
        Hexadecimal string
    """
    if num < 10:
        return str(int(num))
    elif num < 11:
        return "A"
    elif num < 12:
        return "B"
    elif num < 13:
        return "C"
    elif num < 14:
        return "D"
    elif num < 15:
        return "E"
    elif num < 16:
        return "F"


def get_gray_tonality(ev, max_ev):
    inte = math.floor(255.0 - 150.0 * ev / (max_ev + 0.00001))

    n1 = str(get_corr_hex(inte / 16))
    n2 = str(get_corr_hex(inte % 16))

    return "#" + n1 + n2 + n1 + n2 + n1 + n2


def apply(model, measure="frequency", freq="events", classifier="activity", projection="no", count_events=False, parameters=None):
    if parameters is None:
        parameters = {}

    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("pt", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})
    image_format = parameters["format"] if "format" in parameters else "png"

    min_act_freq = parameters["min_act_freq"] if "min_act_freq" in parameters else 5000
    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 1000

    max_ev = 0
    for a in model["activities"]:
        act = model["activities"][a]
        max_ev = max(max_ev, act["events"])

    activities = {}
    for a in model["activities"]:
        act = model["activities"][a]
        ev = act["events"]

        if ev >= min_act_freq:
            act_id = str(uuid.uuid4())
            activities[a] = act_id

            viz.node(act_id, label=a+"\\nE="+str(ev), shape="box", style="filled", fillcolor=get_gray_tonality(ev, max_ev))

    type_index = -1
    for t in model["types_view"]:
        type = model["types_view"][t]
        at_least_one_edge = False
        at_least_one_sa = False
        at_least_one_ea = False

        for e in type["edges"]:
            edge = type["edges"][e]
            ev = edge["events"]

            if ev >= min_edge_freq:
                if e[0] in activities and e[1] in activities:
                    at_least_one_edge = True
                    break

        for a in type["start_activities"]:
            if a in activities:
                at_least_one_sa = True
                break

        for a in type["end_activities"]:
            if a in activities:
                at_least_one_ea = True
                break

        if at_least_one_edge and at_least_one_sa and at_least_one_ea:
            type_index = type_index + 1
            this_color = COLORS[type_index % len(COLORS)]
            sn_id = str(uuid.uuid4())
            en_id = str(uuid.uuid4())
            viz.node(sn_id, t, style="filled", fillcolor=this_color)
            viz.node(en_id, t, style="filled", fillcolor=this_color)

            for a in type["start_activities"]:
                if a in activities:
                    viz.edge(sn_id, activities[a], color=this_color)

            for a in type["end_activities"]:
                if a in activities:
                    viz.edge(activities[a], en_id, color=this_color)

            for e in type["edges"]:
                edge = type["edges"][e]
                ev = edge["events"]
                if ev >= min_edge_freq:
                    if e[0] in activities and e[1] in activities:
                        viz.edge(activities[e[0]], activities[e[1]], label="EC="+str(ev), color=this_color)

    viz.format = image_format

    return viz


def apply_extensive(model, measure="frequency", freq="events", classifier="activity", projection="no", count_events=False, parameters=None):
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

    for index, tk in enumerate(types_keys):
        type_color = COLORS[index % len(COLORS)]
        types_colors[tk] = type_color

    act_nodes = {}
    ordered_activities = sorted(model["activities"])
    for act in ordered_activities:
        fr_ev = model["activities"][act]["events"]
        fr_obj = model["activities"][act]["objects"]
        fr_eo = model["activities"][act]["eo"]

        if fr_ev > min_act_freq:
            act_nodes[act] = str(uuid.uuid4())

            viz.node(act_nodes[act], label=act+"\nE=%d,UO=%d,TO=%d" % (fr_ev, fr_obj, fr_eo), shape="box", penwidth="2", fontsize="27", fontweight="bold")

            for index, tk in enumerate(types_keys):
                t = model["types_view"][tk]
                if act in t["activities"]:
                    this_ev = t["activities"][act]["events"]
                    this_obj = t["activities"][act]["objects"]
                    this_eo = t["activities"][act]["eo"]
                    this_min = t["activities"][act]["min_obj"]
                    this_max = t["activities"][act]["max_obj"]

                    this_node = str(uuid.uuid4())
                    viz.node(this_node, label=act+" ("+tk+")\nE=%d,UO=%d,TO=%d\nmin=%d,max=%d" % (this_ev, this_obj, this_eo, this_min, this_max), shape="tripleoctagon", fontsize="10", style="filled", fillcolor=types_colors[tk])

                    viz.edge(this_node, act_nodes[act], color=types_colors[tk], arrowhead="none")

    for index, tk in enumerate(types_keys):
        t = model["types_view"][tk]
        edges = sorted(list(t["edges"]))
        for edge in edges:
            source = edge[0]
            target = edge[1]
            source_type = model["activities_mapping"][source]
            target_type = model["activities_mapping"][target]

            if source in act_nodes and target in act_nodes:
                fr_ev = t["edges"][edge]["events"]
                fr_obj = t["edges"][edge]["objects"]
                fr_eo = t["edges"][edge]["eo"]

                if fr_ev > min_edge_freq:
                    source_ev = t["activities"][source]["events"]
                    source_obj = t["activities"][source]["objects"]
                    source_eo = t["activities"][source]["eo"]

                    target_ev = t["activities"][source]["events"]
                    target_obj = t["activities"][source]["objects"]
                    target_eo = t["activities"][source]["eo"]

                    this_node = str(uuid.uuid4())
                    viz.node(this_node, shape="none", fontsize="15", fontcolor=types_colors[tk], label="TO=%d,UO=%d,EC=%d" % (fr_eo, fr_obj, fr_ev))
                    viz.edge(act_nodes[source], this_node, color=types_colors[tk], fontcolor=types_colors[tk], taillabel="%" + " = %.2f" % (float(fr_obj)/float(source_obj) * 100.0), fontsize="13", penwidth="4")
                    viz.edge(this_node, act_nodes[target], color=types_colors[tk], fontcolor=types_colors[tk], headlabel="%" + " = %.2f" % (float(fr_obj)/float(target_obj) * 100.0), fontsize="13", penwidth="4")

    """
    FONTSIZE_NODES = '26'
    FONTSIZE_EDGES = '26'
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
    """
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
