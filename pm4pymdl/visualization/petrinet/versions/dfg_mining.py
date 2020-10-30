import uuid
import tempfile
from graphviz import Digraph
from pm4py.objects.petri.petrinet import PetriNet
from statistics import median, mean

COLORS = ["#05B202", "#A13CCD", "#BA0D39", "#39F6C0", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
          "#4C36E9", "#7DB022", "#EDAC54", "#EAC439", "#EAC439", "#1A9C45", "#8A51C4", "#496A63", "#FB9543", "#2B49DD",
          "#13ADA5", "#2DD8C1", "#2E53D7", "#EF9B77", "#06924F", "#AC2C4D", "#82193F", "#0140D3"]


def apply(obj, parameters=None):
    if parameters is None:
        parameters = {}

    image_format = "png"
    if "format" in parameters:
        image_format = parameters["format"]

    filename = tempfile.NamedTemporaryFile(suffix='.gv').name
    g = Digraph("", filename=filename, engine='dot', graph_attr={'bgcolor': 'transparent'})

    nets = obj["nets"]
    replay = obj["replay"]
    act_count = obj["act_count"]
    place_fitness_per_trace_persp = obj["place_fitness_per_trace"]
    group_size_hist_persp = obj["group_size_hist"]
    aggregated_statistics_performance_min = obj["aggregated_statistics_performance_min"]
    aggregated_statistics_performance_median = obj["aggregated_statistics_performance_median"]
    aggregated_statistics_performance_mean = obj["aggregated_statistics_performance_mean"]

    nodes = {}
    places_produced = {}

    for index, persp in enumerate(nets):
        net, im, fm = nets[persp]

        place_fitness_per_trace = place_fitness_per_trace_persp[persp]
        group_size_hist = group_size_hist_persp[persp]

        places_to_consider = [x for x in net.places if x.name not in ["source", "sink"] and x.name in group_size_hist]
        for p in places_to_consider:
            if not p.name in places_produced:
                places_produced[p.name] = 0
            places_produced[p.name] += place_fitness_per_trace[p]["p"]

    for index, persp in enumerate(nets):
        type_color = COLORS[index % len(COLORS)]

        net, im, fm = nets[persp]

        group_size_hist = group_size_hist_persp[persp]

        places_to_consider = [x for x in net.places if x.name not in ["source", "sink"] and x.name in group_size_hist]
        for pl in places_to_consider:
            g.node(pl.name, label=pl.name, shape="box")

        map_ingoing = {}
        map_outgoing = {}

        for tr in net.transitions:
            for source_arc in tr.in_arcs:
                source_place = source_arc.source
                if source_place in places_to_consider:
                    for out_arc in tr.out_arcs:
                        target_place = out_arc.target
                        if target_place in places_to_consider:
                            if source_place not in map_outgoing:
                                map_outgoing[source_place] = set()
                            map_outgoing[source_place].add(target_place)
                            if target_place not in map_ingoing:
                                map_ingoing[target_place] = set()
                            map_ingoing[target_place].add(source_place)

        for tr in net.transitions:
            for source_arc in tr.in_arcs:
                source_place = source_arc.source
                if source_place in places_to_consider:
                    for out_arc in tr.out_arcs:
                        target_place = out_arc.target
                        if target_place in places_to_consider:
                            label = None
                            label = ""
                            label += "SOU freq: mean=%.2f min=%.2f max=%.2f\\neve=%d uniqobj=%d\\n\\n" % (
                                mean(group_size_hist[source_place.name]), min(group_size_hist[source_place.name]),
                                max(group_size_hist[source_place.name]),
                                len(group_size_hist[source_place.name]), sum(group_size_hist[source_place.name]))
                            label += "TAR freq: mean=%.2f min=%.2f max=%.2f\\neve=%d uniqobj=%d\\n\\n" % (
                                mean(group_size_hist[target_place.name]), min(group_size_hist[target_place.name]),
                                max(group_size_hist[target_place.name]),
                                len(group_size_hist[target_place.name]), sum(group_size_hist[target_place.name]))
                            if len(map_outgoing[source_place]) == 1:
                                arrowtail = "dot"
                            else:
                                arrowtail = None
                            if len(map_ingoing[target_place]) == 1:
                                arrowhead = "diamond"
                            else:
                                arrowhead = "curve"

                            if arrowtail is not None and arrowhead is not None:
                                style = "solid"
                            else:
                                style = "dashed"

                            if arrowtail is not None:
                                g.edge(source_place.name, target_place.name, arrowhead=arrowhead, arrowtail=arrowtail,
                                       style=style, label=label, color=type_color, dir="both")
                            else:
                                g.edge(source_place.name, target_place.name, arrowhead=arrowhead, arrowtail=arrowtail,
                                       style=style, color=type_color, label=label)

    g.attr(overlap='false')
    g.attr(fontsize='11')

    g.format = image_format

    return g
