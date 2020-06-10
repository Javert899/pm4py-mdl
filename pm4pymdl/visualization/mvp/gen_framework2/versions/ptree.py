import tempfile
import uuid
from graphviz import Digraph
from pm4py.objects.process_tree import pt_operator
from pm4pymdl.visualization.mvp.gen_framework2.versions import util

COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def repr_tree(tree, viz, current_node, rec_depth, res, acti_map, persp_color, edges_map, activ_freq_map, measure, freq,
              key, classifier, node_shape, projection, parameters):
    """
    Represent a subtree on the GraphViz object

    Parameters
    -----------
    tree
        Current subtree
    viz
        GraphViz object
    current_node
        Father node of the current subtree
    rec_depth
        Reached recursion depth
    parameters
        Possible parameters of the algorithm.

    Returns
    -----------
    gviz
        (partial) GraphViz object
    """
    freq_prefix = "E="
    if freq == "objects":
        freq_prefix = "O="
    elif freq == "eo":
        freq_prefix = "EO="

    for child in tree.children:
        if child.operator is None:
            this_trans_id = str(uuid.uuid4())
            if child.label is None:
                viz.node(this_trans_id, "tau", style='filled', color=persp_color, fontcolor=persp_color,
                         fillcolor="white", shape='box', fixedsize='true', width="2.5",
                     fontsize="8")
            else:
                if not str(child) in acti_map:
                    acti_map[str(child)] = this_trans_id
                    if str(child) in res["activities_repeated"]:
                        viz.node(this_trans_id, str(child) + " (" + freq_prefix + str(res["activities"][str(child)]) + ")",
                                 style="filled", fillcolor="white", shape=node_shape, fixedsize='true', width="2.5",
                     fontsize="8")
                    else:
                        viz.node(this_trans_id, str(child) + " (" + freq_prefix + str(res["activities"][str(child)]) + ")",
                                 style="filled", fillcolor=persp_color, shape=node_shape, fixedsize='true', width="2.5",
                     fontsize="8")
                else:
                    this_trans_id = acti_map[str(child)]
            viz.edge(current_node, this_trans_id, color=persp_color)
        else:
            condition_wo_operator = child.operator == pt_operator.Operator.XOR and len(
                child.children) == 1 and child.children[0].operator is None
            if condition_wo_operator:
                childchild = child.children[0]
                viz.attr('node', shape='box', fixedsize='true', width="2.5",
                         fontsize="8")
                this_trans_id = str(uuid.uuid4())
                if childchild.label is None:
                    viz.node(this_trans_id, str(childchild), style='filled', color=persp_color, fontcolor=persp_color,
                             fillcolor="white", shape='box', fixedsize='true', width="2.5",
                     fontsize="8")
                else:
                    if not str(childchild) in acti_map:
                        acti_map[str(childchild)] = this_trans_id
                        if str(childchild) in res["activities_repeated"]:
                            viz.node(this_trans_id, str(childchild) + " (" + freq_prefix + str(
                                activ_freq_map[key][str(childchild)]) + ")", style="filled", fillcolor="white", shape=node_shape, fixedsize='true', width="2.5",
                     fontsize="8")
                        else:
                            viz.node(this_trans_id, str(childchild) + " (" + freq_prefix + str(
                                activ_freq_map[key][str(childchild)]) + ")", style="filled", fillcolor=persp_color, shape=node_shape, fixedsize='true', width="2.5",
                     fontsize="8")
                    else:
                        this_trans_id = acti_map[str(childchild)]
                viz.edge(current_node, this_trans_id, color=persp_color)
            else:
                op_node_identifier = str(uuid.uuid4())
                viz.node(op_node_identifier, str(child.operator), shape="circle", fixedsize='true', width="0.6",
                         fontsize="14", style="filled", fillcolor=persp_color)
                viz.edge(current_node, op_node_identifier, color=persp_color)
                viz, acti_map = repr_tree(child, viz, op_node_identifier, rec_depth + 1, res, acti_map, persp_color,
                                          edges_map, activ_freq_map,
                                          measure, freq, key, classifier, node_shape, projection, parameters)
    return viz, acti_map


def apply(res, measure="frequency", freq="events", classifier="activity", projection="no", parameters=None):
    if parameters is None:
        parameters = {}

    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 0

    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("pt", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})
    image_format = parameters["format"] if "format" in parameters else "png"
    acti_map = {}

    freq_prefix = "E="
    if freq == "objects":
        freq_prefix = "O="
    elif freq == "eo":
        freq_prefix = "EO="

    node_shape = "box" if classifier == "activity" else "trapezium"

    reference_map = {}
    events_map = {}

    edges_map = {}
    activ_freq_map = {}

    for key in res["models"]:
        edges_map[key] = util.get_edges_map(key, res, measure=measure, variant=freq)
        activ_freq_map[key] = util.get_activity_map_frequency(key, res, variant=freq)
        reference_map[key] = util.get_activity_map_frequency(key, res, variant="objects")
        events_map[key] = util.get_edges_map(key, res, measure="frequency", variant="events")

    edges_map = util.projection_edges_freq(edges_map, events_map, min_edge_freq)
    edges_map = util.projection(edges_map, reference_map, type=projection)

    count = 0
    for key, tree in res["models"].items():
        persp_color = COLORS[count % len(COLORS)]
        count = count + 1

        # add first operator
        if tree.operator:
            op_node_identifier = str(uuid.uuid4())
            viz.node(op_node_identifier, str(tree.operator), shape='circle', fixedsize='true', width="0.6",
                     fontsize="14", style="filled", fillcolor=persp_color)

            viz, acti_map = repr_tree(tree, viz, op_node_identifier, 0, res, acti_map, persp_color, edges_map,
                                      activ_freq_map, measure, freq, key, classifier, node_shape, projection, parameters)
        else:
            this_trans_id = str(uuid.uuid4())
            if tree.label is None:
                viz.node(this_trans_id, "tau", style='filled', color=persp_color, fontcolor=persp_color,
                         fillcolor="white", shape='box', fixedsize='true', width="2.5",
                     fontsize="8")
            else:
                if not str(tree) in acti_map:
                    acti_map[str(tree)] = this_trans_id
                    if str(tree) in res["activities_repeated"]:
                        viz.node(this_trans_id,
                                 str(tree) + " (" + freq_prefix + str(activ_freq_map[key][str(tree)]) + ")",
                                 style="filled", fillcolor="white", shape=node_shape, fixedsize='true', width="2.5",
                     fontsize="8")
                    else:
                        viz.node(this_trans_id,
                                 str(tree) + " (" + freq_prefix + str(activ_freq_map[key][str(tree)]) + ")",
                                 style="filled", fillcolor=persp_color, shape=node_shape, fixedsize='true', width="2.5",
                     fontsize="8")
                else:
                    this_trans_id = acti_map[str(tree)]

    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz
