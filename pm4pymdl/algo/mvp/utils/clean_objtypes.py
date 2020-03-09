import pandas as pd


def perfom_cleaning(df, parameters=None):
    if parameters is None:
        parameters = {}
    allowed_activities = parameters["allowed_activities"] if "allowed_activities" in parameters else None
    if allowed_activities is not None:
        persps = [x for x in df.columns if not x.startswith("event_")]
        collation = []
        for persp in persps:
            red_df = df.dropna(subset=[persp])
            if persp in allowed_activities:
                red_df = red_df[red_df["event_activity"].isin(allowed_activities[persp])]
                collation.append(red_df)
        if collation:
            df = pd.concat(collation)
        else:
            df = pd.DataFrame()
    return df
