import tempfile
import uuid
from graphviz import Digraph
from pm4pymdl.visualization.mvp.gen_framework2.versions import util

COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def apply(res, measure="frequency", freq="events", parameters=None):
    if parameters is None:
        parameters = {}

    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("pt", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})
    image_format = parameters["format"] if "format" in parameters else "png"
    acti_map = {}

    freq_prefix = "E="
    if freq == "objects":
        freq_prefix = "O="
    elif freq == "eo":
        freq_prefix = "EO="

    count = 0

    edges_map = {}
    activ_freq_map = {}

    for key in res["models"]:
        edges_map[key] = util.get_edges_map(key, res, measure=measure, variant=freq)
        activ_freq_map[key] = util.get_activity_map_frequency(key, res, variant=freq)

    for key, model in res["models"].items():
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
                    viz.node(act_id, act+" ("+freq_prefix+str(activ_freq_map[key][act])+")", style='filled', fillcolor="white", color=persp_color)
                else:
                    viz.node(act_id, act+" ("+freq_prefix+str(activ_freq_map[key][act])+")", style='filled', fillcolor=persp_color)

            if act in res["start_activities"][key]:
                viz.edge(sn_uuid, act_id, color=persp_color)

            if act in res["end_activities"][key]:
                viz.edge(act_id, en_uuid, color=persp_color)

        for k in edges_map[key]:
            if measure == "frequency":
                viz.edge(acti_map[k[0]], acti_map[k[1]], xlabel=freq_prefix+str(edges_map[key][k]), color=persp_color, fontcolor=persp_color)
            elif measure == "performance":
                viz.edge(acti_map[k[0]], acti_map[k[1]], xlabel=freq_prefix+"P="+util.human_readable_stat(edges_map[key][k]), color=persp_color, fontcolor=persp_color)
    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz
