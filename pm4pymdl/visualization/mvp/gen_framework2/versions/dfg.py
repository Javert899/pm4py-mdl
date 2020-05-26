import tempfile
import uuid
from graphviz import Digraph
from collections import Counter

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

    for key, model in res["models"].items():
        edges = [x for x in res["edges"] if x[0] == key]
        edges_map = {}
        for x in edges:
            k = (x[1], x[2])
            if k not in edges_map:
                edges_map[k] = Counter()
            edges_map[k][(x[3], x[4])] += res["edges"][x]
        for k in edges_map:
            edges_map[k] = sum(edges_map[k].values())
        persp_color = COLORS[count % len(COLORS)]
        count = count + 1

        viz.attr('node', shape='circle', fixedsize='true', width='0.75')
        sn_uuid = str(uuid.uuid4())
        sn = viz.node(sn_uuid, "", style="filled", fillcolor="green")
        en_uuid = str(uuid.uuid4())
        en = viz.node(en_uuid, "", style="filled", fillcolor="orange")

        viz.attr('node', shape='box', width='3.8')

        for act in model:
            act_id = str(id(act))

            if act in acti_map:
                pass
            else:
                acti_map[act] = act_id
                if act in res["activities_repeated"]:
                    viz.node(act_id, act, style='filled', fillcolor="white", color=persp_color)
                else:
                    viz.node(act_id, act, style='filled', fillcolor=persp_color)

            if act in res["start_activities"][key]:
                viz.edge(sn_uuid, act_id, color=persp_color)

            if act in res["end_activities"][key]:
                viz.edge(act_id, en_uuid, color=persp_color)

        for k in edges_map:
            viz.edge(acti_map[k[0]], acti_map[k[1]], xlabel=str(edges_map[k]), color=persp_color)

    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz
