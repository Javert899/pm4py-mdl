from graphviz import Digraph
import uuid
import tempfile
from pm4pymdl.visualization.mvp.gen_framework.versions import dfg_clean


COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def apply(model, parameters=None):
    if parameters is None:
        parameters = {}

    min_node_freq = parameters["min_node_freq"] if "min_node_freq" in parameters else 0
    min_edge_freq = parameters["min_edge_freq"] if "min_edge_freq" in parameters else 0
    dfg_cleaning_threshold = parameters["dfg_cleaning_threshold"] if "dfg_cleaning_threshold" in parameters else -1
    max_edge_ratio = parameters["max_edge_ratio"] if "max_edge_ratio" in parameters else 100000000

    image_format = "png"
    if "format" in parameters:
        image_format = parameters["format"]

    filename = tempfile.NamedTemporaryFile(suffix='.gv').name
    g = Digraph("", filename=filename, engine='dot', graph_attr={'bgcolor': 'transparent'})

    nodes_map = {}

    count_nodes = 0

    all_activities = model.get_all_activities()

    for activity in all_activities:
        this_uuid = str(uuid.uuid4())

        if model.node_freq[activity] >= min_node_freq:
            g.node(this_uuid, activity + " (" + str(model.node_freq[activity]) + ")")
            nodes_map[activity] = this_uuid

            count_nodes = count_nodes + 1

    persp_list = sorted(list(model.edge_freq))
    count_edges = 0

    for index, persp in enumerate(persp_list):
        color = COLORS[index % len(COLORS)]
        edges = model.edge_freq[persp]

        if dfg_cleaning_threshold > -1:
            edges = dfg_clean.clean_dfg_based_on_noise_thresh(edges, model.node_freq, dfg_cleaning_threshold)

        for edge in edges:
            if edge.split("@@")[0] in nodes_map and edge.split("@@")[1] in nodes_map:
                if edges[edge] >= min_edge_freq:
                    act1 = edge.split("@@")[0]
                    act2 = edge.split("@@")[1]
                    act1_corr = nodes_map[act1]
                    act2_corr = nodes_map[act2]

                    if max(edges[edge]/model.node_freq[act1], edges[edge]/model.node_freq[act2]) < max_edge_ratio:
                        g.edge(act1_corr, act2_corr, persp + " ("+str(edges[edge])+")", color=color, fontcolor=color)
                        count_edges = count_edges + 1

    if "count_nodes" in parameters and "count_edges" in parameters:
        print("Type 1 nodes",count_nodes,"recall",count_nodes/parameters["count_nodes"])
        print("Type 1 edges",count_edges,"recall",count_edges/parameters["count_edges"])
    else:
        print("Type 1 nodes",count_nodes)
        print("Type 1 edges",count_edges)

    g.attr(overlap='false')
    g.attr(fontsize='11')

    g.format = image_format

    return g
