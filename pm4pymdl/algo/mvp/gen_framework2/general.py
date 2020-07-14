from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl, clean_objtypes
import pandas as pd

def preprocess(df, parameters=None):
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

    #df = clean_objtypes.perfom_cleaning(df, parameters=parameters)

    if len(df) == 0:
        df = pd.DataFrame({"event_id": [], "event_activity": []})

    return df
