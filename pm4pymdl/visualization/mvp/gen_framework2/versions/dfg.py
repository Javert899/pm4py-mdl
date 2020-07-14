import tempfile
import uuid
from graphviz import Digraph
from pm4pymdl.visualization.mvp.gen_framework2.versions import util

COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def get_label(act):
    return act

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

    node_shape = "box" if classifier == "activity" else "box3d"

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
    for k in edges_map:
        if len(edges_map[k]) == 0:
            if k in reference_map:
                del reference_map[k]
    edges_map, acti_assignation_map = util.projection(edges_map, reference_map, type=projection, return_acti_map=True)

    count = 0
    for key, model in res["models"].items():
        if len(events_map[key]) > 0:
            persp_color = COLORS[count % len(COLORS)]
            count = count + 1

            for act in model:
                act_id = str(id(act))

                if key == acti_assignation_map[act]:
                    acti_map[act] = act_id
                    if act in res["activities_repeated"]:
                        label = get_label(act)
                        label = get_label(act)+"\n("+freq_prefix+str(activ_freq_map[key][act])+")"
                        viz.node(act_id, label, style='filled', fillcolor="white", color=persp_color, shape=node_shape, width='1.3')
                    else:
                        label = get_label(act)
                        label = get_label(act)+"\n("+freq_prefix+str(activ_freq_map[key][act])+")"
                        viz.node(act_id, label, style='filled', fillcolor=persp_color, shape=node_shape, width='1.3')
    count = 0
    for key, model in res["models"].items():
        if len(events_map[key]) > 0:
            persp_color = COLORS[count % len(COLORS)]
            count = count + 1

            sn_uuid = str(uuid.uuid4())
            viz.node(sn_uuid, str(key), color=persp_color, fontcolor=persp_color, shape='underline', fixedsize='true', width='0.75')
            en_uuid = str(uuid.uuid4())
            viz.node(en_uuid, str(key), color=persp_color, fontcolor=persp_color, shape='none', fixedsize='true', width='0.75')

            for act in model:
                if act in res["start_activities"][key]:
                    viz.edge(sn_uuid, acti_map[act], color=persp_color)

                if act in res["end_activities"][key]:
                    viz.edge(acti_map[act], en_uuid, color=persp_color)

    count = 0
    for key, model in res["models"].items():
        if len(events_map[key]) > 0:
            persp_color = COLORS[count % len(COLORS)]
            count = count + 1

            for k in edges_map[key]:
                if measure == "frequency":
                    viz.edge(acti_map[k[0]], acti_map[k[1]], xlabel=freq_prefix+str(edges_map[key][k]), color=persp_color, fontcolor=persp_color)
                elif measure == "performance":
                    viz.edge(acti_map[k[0]], acti_map[k[1]], xlabel=freq_prefix+"P="+util.human_readable_stat(edges_map[key][k]), color=persp_color, fontcolor=persp_color)

    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz
