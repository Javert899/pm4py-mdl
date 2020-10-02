from pm4pymdl.algo.mvp.gen_framework.models import model_builder as model_factory
from pm4pymdl.algo.mvp.gen_framework.rel_events import rel_events_builder as rel_ev_factory
from pm4pymdl.algo.mvp.gen_framework.rel_activities import rel_activities_builder as rel_act_factory
from pm4pymdl.algo.mvp.gen_framework.node_freq import node_freq_builder as node_freq_factory
from pm4pymdl.algo.mvp.gen_framework.edge_freq import edge_freq_builder as edge_freq_factory
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl, clean_objtypes
import pandas as pd


MODEL1 = model_factory.MODEL1
MODEL2 = model_factory.MODEL2
MODEL3 = model_factory.MODEL3
REL_DFG = rel_ev_factory.REL_DFG
BEING_PRODUCED = rel_ev_factory.BEING_PRODUCED
TYPE1 = node_freq_factory.TYPE1
TYPE11 = edge_freq_factory.TYPE11
TYPE12 = edge_freq_factory.TYPE12

COLORS = {REL_DFG: "#000000"}

def apply(df, model_type_variant=MODEL1, rel_ev_variant=REL_DFG, node_freq_variant=TYPE1, edge_freq_variant=TYPE11, parameters=None):
    if parameters is None:
        parameters = {}

    conversion_needed = False

    try:
        if df.type == "succint":
            conversion_needed = True
    except:
        pass

    if len(df) == 0:
        df = pd.DataFrame({"event_id": [], "event_activity": []})

    if conversion_needed:
        df = succint_mdl_to_exploded_mdl.apply(df)

    df = clean_objtypes.perfom_cleaning(df, parameters=parameters)

    if len(df) == 0:
        df = pd.DataFrame({"event_id": [], "event_activity": []})

    model = model_factory.apply(df, variant=model_type_variant)
    rel_ev = rel_ev_factory.apply(df, model, variant=rel_ev_variant)
    rel_act = rel_act_factory.apply(df, model, rel_ev)
    node_freq = node_freq_factory.apply(df, model, rel_ev, rel_act, variant=node_freq_variant)
    edge_freq = edge_freq_factory.apply(df, model, rel_ev, rel_act, variant=edge_freq_variant)

    model.set_rel_ev(rel_ev)
    model.set_rel_act(rel_act)
    model.set_node_freq(node_freq)
    model.set_edge_freq(edge_freq)

    return model
