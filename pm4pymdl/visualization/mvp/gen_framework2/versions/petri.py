import tempfile
import uuid
from graphviz import Digraph
from pm4py.objects.process_tree import pt_operator


COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def apply(res, parameters=None):
    if parameters is None:
        parameters = {}

    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("pt", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})
    image_format = parameters["format"] if "format" in parameters else "png"
    acti_map = {}

    count = 0

    for model in res["models"].values():
        persp_color = COLORS[count % len(COLORS)]
        count = count + 1

        net, im, fm = model

        # places
        viz.attr('node', shape='circle', fixedsize='true', width='0.75')
        for p in net.places:
            viz.node(str(id(p)), "", style="filled", fillcolor=persp_color)

        # transitions
        viz.attr('node', shape='box')
        for t in net.transitions:
            trans_id = str(id(t))
            if t.label is None:
                viz.node(trans_id, "", style='filled', fillcolor="black", color=persp_color)
            elif t.label in acti_map:
                trans_id = acti_map[t.label]
            else:
                if t.label in res["activities_repeated"]:
                    viz.node(trans_id, t.label+" ("+str(res["activities"][t.label])+")", style="filled", fillcolor="white")
                else:
                    viz.node(trans_id, t.label+" ("+str(res["activities"][t.label])+")", style="filled", fillcolor=persp_color)
                acti_map[t.label] = trans_id
            for arc in t.in_arcs:
                p = arc.source
                viz.edge(str(id(p)), trans_id, color=persp_color)
            for arc in t.out_arcs:
                p = arc.target
                viz.edge(trans_id, str(id(p)), color=persp_color)

    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz
