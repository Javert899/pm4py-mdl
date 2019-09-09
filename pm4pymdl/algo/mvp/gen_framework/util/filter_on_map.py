import pandas as pd

DEFAULT_ACTIVITY_KEY = "event_activity"


def apply(df, map, activity_key=DEFAULT_ACTIVITY_KEY, second_required=False, timestamp_required=True,
          merge_required=False, parameters=None):
    if parameters is None:
        parameters = {}

    df_list = []

    class_dest = {}

    for act in map:
        target = map[act]
        if target not in class_dest:
            class_dest[target] = []
        class_dest[target].append(act)

    for cl in class_dest:
        if cl in df.columns:
            cols_to_include0 = ["event_id", "event_activity", cl]
            if timestamp_required:
                cols_to_include0.append("event_timestamp")

            if not second_required:
                cols_to_include = cols_to_include0
            else:
                cols_to_include = [x for x in cols_to_include0] + [x + "_2" for x in cols_to_include0]

            if activity_key not in cols_to_include:
                cols_to_include.append(activity_key)
            if merge_required:
                cols_to_include.append("event_activity_merge")

            red_df = df[cols_to_include].dropna()
            red_df = red_df[red_df[activity_key].isin(class_dest[cl])]

            df_list.append(red_df)

    new_df = pd.concat(df_list).reset_index()

    return new_df
