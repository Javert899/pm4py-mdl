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
    group_size = obj["group_size_hist_replay"]
    aligned_traces = obj["aligned_traces"]
    place_fitness_per_trace_persp = obj["place_fitness_per_trace"]
    group_size_hist_persp = obj["group_size_hist"]
    aggregated_statistics_frequency = obj["aggregated_statistics_frequency"]
    aggregated_statistics_performance_min = obj["aggregated_statistics_performance_min"]
    aggregated_statistics_performance_max = obj["aggregated_statistics_performance_max"]
    aggregated_statistics_performance_median = obj["aggregated_statistics_performance_median"]
    aggregated_statistics_performance_mean = obj["aggregated_statistics_performance_mean"]
    aligned_traces = obj["aligned_traces"]

    all_objs = {}
    trans_names = {}

    for index, persp in enumerate(nets):
        orig_act_count = {}
        orig_arc_count = {}

        color = COLORS[index % len(COLORS)]
        net, im, fm = nets[persp]
        rr = replay[persp]
        ac = act_count[persp]
        gs = group_size[persp]

        ag_freq = aggregated_statistics_frequency[persp]
        ag_perf_min = aggregated_statistics_performance_min[persp]
        ag_perf_max = aggregated_statistics_performance_max[persp]
        ag_perf_median = aggregated_statistics_performance_median[persp]
        ag_perf_mean = aggregated_statistics_performance_mean[persp]
        al_trac = aligned_traces[persp]
        place_fitness_per_trace = place_fitness_per_trace_persp[persp]
        group_size_hist = group_size_hist_persp[persp]

        for pl in place_fitness_per_trace:
            if place_fitness_per_trace[pl]["c"] + place_fitness_per_trace[pl]["r"] > place_fitness_per_trace[pl]["p"] + \
                    place_fitness_per_trace[pl]["m"]:
                place_fitness_per_trace[pl]["m"] += place_fitness_per_trace[pl]["c"] + place_fitness_per_trace[pl][
                    "r"] - \
                                                    place_fitness_per_trace[pl]["p"]
            elif place_fitness_per_trace[pl]["c"] + place_fitness_per_trace[pl]["r"] < place_fitness_per_trace[pl][
                "p"] + \
                    place_fitness_per_trace[pl]["m"]:
                place_fitness_per_trace[pl]["r"] += place_fitness_per_trace[pl]["p"] + place_fitness_per_trace[pl][
                    "m"] - \
                                                    place_fitness_per_trace[pl]["c"]

        # print("ag_freq",ag_freq)
        # print("ag_perf_min",ag_perf_min)
        # print("ag_perf_max",ag_perf_max)
        # print("ag_perf_median",ag_perf_median)
        # print("ag_perf_mean",ag_perf_mean)
        # print("al_trac",al_trac)
        # print("pl_fitness",place_fitness_per_trace)

        # input()

        for pl in net.places:
            this_uuid = str(uuid.uuid4())
            pl_str = "p=%d c=%d m=%d r=%d" % (place_fitness_per_trace[pl]["p"], place_fitness_per_trace[pl]["c"], place_fitness_per_trace[pl]["m"], place_fitness_per_trace[pl]["r"])
            if pl in im:
                g.node(this_uuid, pl_str, shape="circle", fillcolor=color, style="filled", fontsize="8.0", labelfontsize="8.0")
            elif pl in fm:
                g.node(this_uuid, pl_str, shape="egg", fillcolor=color, style="filled", fontsize="8.0",
                       labelfontsize="8.0")
            else:
                g.node(this_uuid, pl_str, shape="ellipse", fillcolor=color, style="filled", fontsize="8.0",
                       labelfontsize="8.0")
            all_objs[pl] = this_uuid

        for tr in net.transitions:
            this_uuid = str(uuid.uuid4())
            if tr.label is None:
                g.node(this_uuid, "", shape="box", fillcolor="#000000", style="filled")
                all_objs[tr] = this_uuid
            elif tr.label not in trans_names:
                count = rr[tr]["label"].split("(")[-1].split("]")[0]
                orig_act_count[tr] = count
                this_act_count = "%d" % (float(ac[tr.label]))
                g.node(this_uuid, tr.label + " \\n" + this_act_count + "", shape="box")
                trans_names[tr.label] = this_uuid
                all_objs[tr] = this_uuid
            else:
                all_objs[tr] = trans_names[tr.label]

        for arc in net.arcs:
            this_uuid = str(uuid.uuid4())

            source_node = arc.source
            target_node = arc.target

            if arc in rr:

                arc_count = float(rr[arc]['label'])

                if arc in ag_perf_min:
                    perf_str = "\\npmin=%s pmax=%s pmedian=%s pmean=%s" % (
                    ag_perf_min[arc]['label'], ag_perf_max[arc]['label'], ag_perf_median[arc]['label'],
                    ag_perf_mean[arc]['label'])
                else:
                    perf_str = ""

                if type(source_node) is PetriNet.Place:
                    if target_node.label is not None and ac[target_node.label] > 0:
                        pre = "EVGS: min=%d max=%d median=%d mean=%.2f eve=%d obj=%d\\n" % (min(group_size_hist[target_node.label]), max(group_size_hist[target_node.label]),
                                                                              median(group_size_hist[target_node.label]), mean(group_size_hist[target_node.label]), len(group_size_hist[target_node.label]), sum(group_size_hist[target_node.label]))
                        int = "TR: %.2f (rep_arc_count=%d, rep_act_count=%d)" % (arc_count / ac[target_node.label], arc_count, ac[target_node.label])
                        ratio = pre + perf_str + ""
                    else:
                        ratio = "1.00" + perf_str + ""
                else:
                    if source_node.label is not None and ac[source_node.label] > 0:
                        pre = "EVGS: min=%d max=%d median=%d mean=%.2f eve=%d obj=%d\\n" % (min(group_size_hist[source_node.label]), max(group_size_hist[source_node.label]),
                                                                              median(group_size_hist[source_node.label]), mean(group_size_hist[source_node.label]), len(group_size_hist[source_node.label]), sum(group_size_hist[source_node.label]))
                        int = "TR=%.2f (rep_arc_count=%d, rep_act_count=%d)" % (arc_count / ac[source_node.label], arc_count, ac[source_node.label])
                        ratio = pre + perf_str + ""
                    else:
                        ratio = "1.00" + perf_str + ""

                g.edge(all_objs[source_node], all_objs[target_node], label=ratio, penwidth=str(rr[arc]['penwidth']),
                       color=color, fontsize="8.0")

                all_objs[arc] = this_uuid
            else:
                g.edge(all_objs[source_node], all_objs[target_node], label="", color=color, fontsize="8.0")

    g.attr(overlap='false')
    g.attr(fontsize='11')

    g.format = image_format

    return g
