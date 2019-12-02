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
                """place_fitness_per_trace[pl]["m"] += place_fitness_per_trace[pl]["c"] + place_fitness_per_trace[pl][
                    "r"] - \
                                                    place_fitness_per_trace[pl]["p"]"""
                place_fitness_per_trace[pl]["c"] = place_fitness_per_trace[pl]["p"] + place_fitness_per_trace[pl]["m"] - \
                                                   place_fitness_per_trace[pl]["r"]
            elif place_fitness_per_trace[pl]["c"] + place_fitness_per_trace[pl]["r"] < place_fitness_per_trace[pl][
                "p"] + \
                    place_fitness_per_trace[pl]["m"]:
                """place_fitness_per_trace[pl]["r"] += place_fitness_per_trace[pl]["p"] + place_fitness_per_trace[pl][
                    "m"] - \
                                                    place_fitness_per_trace[pl]["c"]"""
                place_fitness_per_trace[pl]["p"] = place_fitness_per_trace[pl]["c"] + place_fitness_per_trace[pl]["r"] - \
                                                   place_fitness_per_trace[pl]["m"]

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
            pl_str = "p=%d r=%d\\nc=%d m=%d" % (
                place_fitness_per_trace[pl]["p"], place_fitness_per_trace[pl]["r"], place_fitness_per_trace[pl]["c"],
                place_fitness_per_trace[pl]["m"])
            is_double_circled = False
            for arc in pl.in_arcs:
                if arc.source.label in group_size_hist:
                    if len(group_size_hist[arc.source.label]) != sum(group_size_hist[arc.source.label]):
                        is_double_circled = True
            for arc in pl.out_arcs:
                if arc.target.label in group_size_hist:
                    if len(group_size_hist[arc.target.label]) != sum(group_size_hist[arc.target.label]):
                        is_double_circled = True
            if is_double_circled:
                if pl in im:
                    g.node(this_uuid, pl_str, shape="doublecircle", style="filled", color=color, fillcolor=color,
                           fontsize="13.0", labelfontsize="13.0", penwidth="3.0")
                elif pl in fm:
                    g.node(this_uuid, pl_str, shape="doublecircle", style="filled", color=color, fillcolor=color,
                           fontsize="13.0", labelfontsize="13.0", penwidth="3.0")
                else:
                    g.node(this_uuid, pl_str, shape="doublecircle", color=color, fontsize="13.0", labelfontsize="13.0", penwidth="3.0")
            else:
                if pl in im:
                    g.node(this_uuid, pl_str, shape="circle", style="filled", fillcolor=color, color=color,
                           fontsize="13.0", labelfontsize="13.0")
                elif pl in fm:
                    g.node(this_uuid, pl_str, shape="circle", style="filled", color=color, fillcolor=color,
                           fontsize="13.0", labelfontsize="13.0")
                else:
                    g.node(this_uuid, pl_str, shape="circle", color=color, fontsize="13.0", labelfontsize="13.0")
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
                g.node(this_uuid, tr.label + " \\n" + this_act_count + "", shape="box", fontsize="36.0",
                       labelfontsize="36.0")
                trans_names[tr.label] = this_uuid
                all_objs[tr] = this_uuid
            else:
                all_objs[tr] = trans_names[tr.label]

        for arc in net.arcs:
            this_uuid = str(uuid.uuid4())

            source_node = arc.source
            target_node = arc.target
            to_double_arc = False

            if arc in rr:

                arc_count = float(rr[arc]['label'])

                if arc in ag_perf_min:
                    perf_str = "\\nperf: median=%s mean=%s" % (
                        ag_perf_median[arc]['label'],
                        ag_perf_mean[arc]['label'])
                else:
                    perf_str = ""

                if type(source_node) is PetriNet.Place:
                    if True or (target_node.label is not None and ac[target_node.label] > 0):
                        # if ac[target_node.label] > 0:
                        if target_node.label in group_size_hist and max(group_size_hist[target_node.label]) > 1:
                            pre = "freq: median=%d mean=%.2f\\neve=%d uniqobj=%d\\n" % (
                                median(group_size_hist[target_node.label]), mean(group_size_hist[target_node.label]),
                                len(group_size_hist[target_node.label]), sum(group_size_hist[target_node.label]))
                        else:
                            # pre="freq: %d\\n" % len(group_size_hist[target_node.label])
                            pre = "uniqobj=" + str(int(arc_count))
                        if target_node.label in group_size_hist and sum(group_size_hist[target_node.label]) != len(
                                group_size_hist[target_node.label]):
                            to_double_arc = True

                        ratio = pre + perf_str + ""
                    else:
                        # ratio = "1.00" + perf_str + ""
                        ratio = "freq: " + str(int(arc_count))
                else:
                    if True or (source_node.label is not None and ac[source_node.label] > 0):
                        # if ac[source_node.label] > 0:
                        if source_node.label in group_size_hist and max(group_size_hist[source_node.label]) > 1:
                            pre = "freq: median=%d mean=%.2f\\neve=%d uniqobj=%d\\n" % (
                                median(group_size_hist[source_node.label]), mean(group_size_hist[source_node.label]),
                                len(group_size_hist[source_node.label]), sum(group_size_hist[source_node.label]))
                        else:
                            # pre="freq: %d\\n" % len(group_size_hist[source_node.label])
                            pre = "uniqobj=" + str(int(arc_count))
                        if source_node.label in group_size_hist and sum(group_size_hist[source_node.label]) != len(
                                group_size_hist[source_node.label]):
                            to_double_arc = True

                        ratio = pre + perf_str + ""
                    else:
                        # ratio = "1.00" + perf_str + ""
                        ratio = "freq: " + str(int(arc_count))

                if to_double_arc:
                    g.edge(all_objs[source_node], all_objs[target_node], label=ratio, penwidth=str(3.0*float(rr[arc]['penwidth'])),
                           color=color + ":white:" + color, fontsize="13.0")
                else:
                    g.edge(all_objs[source_node], all_objs[target_node], label=ratio, penwidth=str(1.2*float(rr[arc]['penwidth'])),
                           color=color, fontsize="13.0")

                all_objs[arc] = this_uuid
            else:
                g.edge(all_objs[source_node], all_objs[target_node], label="", color=color, fontsize="11.0")

    g.attr(overlap='false')
    g.attr(fontsize='11')

    g.format = image_format

    return g
