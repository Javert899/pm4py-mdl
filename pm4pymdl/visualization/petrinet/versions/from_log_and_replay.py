import uuid
import tempfile
from graphviz import Digraph
from pm4py.objects.petri.petrinet import PetriNet


COLORS = ["#05B202", "#A13CCD", "#39F6C0", "#BA0D39", "#E90638", "#07B423", "#306A8A", "#678225", "#2742FE", "#4C9A75",
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
    act_count = obj["act_count_replay"]
    group_size = obj["group_size_hist_replay"]

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

        for pl in net.places:
            this_uuid = str(uuid.uuid4())
            if pl in im:
                g.node(this_uuid, "", shape="circle", fillcolor=color, style="filled")
            elif pl in fm:
                g.node(this_uuid, "", shape="circle", fillcolor=color, style="filled")
            else:
                g.node(this_uuid, "", shape="circle", fillcolor=color, style="filled")
            all_objs[pl] = this_uuid

        for tr in net.transitions:
            this_uuid = str(uuid.uuid4())
            if tr.label is None:
                g.node(this_uuid, "", shape="box", fillcolor="#000000", style="filled")
                all_objs[tr] = this_uuid
            elif tr.label not in trans_names:
                count = rr[tr]["label"].split("(")[-1].split("]")[0]
                orig_act_count[tr] = count
                this_act_count = "%.2f" % (float(ac[tr.label]))
                g.node(this_uuid, "'" + tr.label+" <<"+this_act_count+">>'", shape="box")
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

                if type(source_node) is PetriNet.Place:
                    if target_node.label is not None and ac[target_node.label] > 0:
                        ratio = "'<<%.2f>>'" % (arc_count / ac[target_node.label])
                    else:
                        ratio = "'<<1.00>>'"
                else:
                    if source_node.label is not None and ac[source_node.label] > 0:
                        ratio = "'<<%.2f>>'" % (arc_count / ac[source_node.label])
                    else:
                        ratio = "'<<1.00>>'"

                g.edge(all_objs[source_node], all_objs[target_node], label=ratio, penwidth=str(rr[arc]['penwidth']), color=color)

                all_objs[arc] = this_uuid
            else:
                g.edge(all_objs[source_node], all_objs[target_node], label="", color=color)

    g.attr(overlap='false')
    g.attr(fontsize='11')

    g.format = image_format

    return g
