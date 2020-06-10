import tempfile
import uuid
from graphviz import Digraph
from pm4pymdl.visualization.mvp.gen_framework2.versions import util


COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


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
    edges_map = {}
    activ_freq_map = {}

    for key in res["models"]:
        edges_map[key] = util.get_edges_map(key, res, measure=measure, variant=freq)
        activ_freq_map[key] = util.get_activity_map_frequency(key, res, variant=freq)
        reference_map[key] = util.get_activity_map_frequency(key, res, variant="objects")

    edges_map = util.projection(edges_map, reference_map, type=projection)

    count = 0

    for model in res["models"].values():
        persp_color = COLORS[count % len(COLORS)]
        count = count + 1

        viz.attr('node', shape='box', width='3.8')


    viz.attr(overlap='false')
    viz.attr(fontsize='11')
    viz.format = image_format

    return viz
