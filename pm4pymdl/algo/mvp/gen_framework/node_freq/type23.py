from pm4pymdl.algo.mvp.gen_framework.util import filter_on_map

def apply(df, model, rel_ev, rel_act, parameters=None):
    if parameters is None:
        parameters = {}

    new_df = filter_on_map.apply(df, model.map)

    return dict(new_df["event_activity"].value_counts())
