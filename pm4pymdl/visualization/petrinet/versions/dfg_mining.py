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
            if not p in places_produced:
                places_produced[p.name] = 0
            places_produced[p.name] += place_fitness_per_trace[p]["p"]

    for index, persp in enumerate(nets):
        net, im, fm = nets[persp]

        group_size_hist = group_size_hist_persp[persp]

        places_to_consider = [x for x in net.places if x.name not in ["source", "sink"] and x.name in group_size_hist]
        for pl in places_to_consider:
            g.node(pl.name, label=pl.name+" (O="+str(places_produced[pl.name])+")", shape="box")

        for tr in net.transitions:
            for source_arc in tr.in_arcs:
                source_place = source_arc.source
                if source_place in places_to_consider:
                    for out_arc in tr.out_arcs:
                        target_place = out_arc.target
                        if target_place in places_to_consider:
                            label = None
                            label = ""
                            label += "SOU freq: median=%d mean=%.2f\\neve=%d uniqobj=%d\\n\\n" % (
                            median(group_size_hist[source_place.name]), mean(group_size_hist[source_place.name]),
                            len(group_size_hist[source_place.name]), sum(group_size_hist[source_place.name]))
                            label += "TAR freq: median=%d mean=%.2f\\neve=%d uniqobj=%d\\n\\n" % (
                            median(group_size_hist[target_place.name]), mean(group_size_hist[target_place.name]),
                            len(group_size_hist[target_place.name]), sum(group_size_hist[target_place.name]))
                            g.edge(source_place.name, target_place.name, label=label)

    g.attr(overlap='false')
    g.attr(fontsize='11')

    g.format = image_format

    return g
