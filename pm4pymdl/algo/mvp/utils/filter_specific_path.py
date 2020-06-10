import pandas as pd
from pm4pymdl.algo.mvp.utils import filter_metaclass
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl


def apply(df, obj_type, act1, act2, minp, maxp, parameters=None):
    try:
        if df.type == "succint":
            df = succint_mdl_to_exploded_mdl.apply(df)
    except:
        pass

    if parameters is None:
        parameters = {}

    cols = [x for x in df.columns if x.startswith("event_")] + [obj_type]
    red_df = df[cols].dropna(subset=[obj_type])
    red_df = red_df.sort_values([obj_type, "event_timestamp"])
    red_df_shifted = red_df.shift(-1)
    red_df_shifted.columns = [str(col) + '_2' for col in red_df_shifted.columns]
    stacked_df = pd.concat([red_df, red_df_shifted], axis=1)
    stacked_df["@@path"] = stacked_df["event_activity"] + "," + stacked_df["event_activity_2"]
    stacked_df["@@diff"] = (stacked_df["event_timestamp_2"] - stacked_df["event_timestamp"]).astype('timedelta64[s]')
    stacked_df = stacked_df[stacked_df["@@path"] == act1+","+act2]
    #stacked_df.info()
    #print(minp, type(minp))
    #print(maxp, type(maxp))
    #print(stacked_df["@@diff"])
    stacked_df = stacked_df[minp <= stacked_df["@@diff"]]
    stacked_df = stacked_df[stacked_df["@@diff"] <= maxp]
    filt_df = red_df[red_df["event_id"].isin(stacked_df["event_id"]) | red_df["event_id"].isin(stacked_df["event_id_2"])]
    return filter_metaclass.do_filtering(df, filt_df)
