from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics


def get(df):
    try:
        if df.type == "succint":
            df = succint_mdl_to_exploded_mdl.apply(df)
    except:
        pass
    activ = dict(df.groupby("event_id").first()["event_activity"].value_counts())
    max_activ_freq = max(activ.values()) if len(activ.values()) > 0 else 0
    dfg = df_statistics.get_dfg_graph(red_df, activity_key="event_activity", timestamp_key="event_timestamp",
                                      case_id_glue=persp)
    max_edge_freq = max(dfg.values()) if len(dfg) > 0 else 0
    return {"max_activ_freq": max_activ_freq, "max_edge_freq": max_edge_freq}

