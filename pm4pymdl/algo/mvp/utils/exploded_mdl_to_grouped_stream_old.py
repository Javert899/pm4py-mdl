def apply(df, remove_common=False, include_activity_timest_in_key=False):
    """
    Dataframe to grouped stream

    Parameters
    -------------
    df
        Dataframe

    Returns
    -------------
    grouped_stream
        Grouped stream of events
    """
    stream = df.to_dict('r')
    grouped_stream = {}

    i = 0
    while i < len(stream):
        keys = list(stream[i].keys())
        for key in keys:
            if str(stream[i][key]) == "nan" or str(stream[i][key]) == "None":
                del stream[i][key]
        if "event_id" in stream[i] and "event_activity" in stream[i] and "event_timestamp" in stream[i]:
            event_id = stream[i]["event_id"]
            event_activity = stream[i]["event_activity"]
            event_timestamp = stream[i]["event_timestamp"]
            if include_activity_timest_in_key:
                if (event_id, event_activity, event_timestamp) not in grouped_stream:
                    grouped_stream[(event_id, event_activity, event_timestamp)] = []
            else:
                if event_id not in grouped_stream:
                    grouped_stream[event_id] = []
            if remove_common:
                del stream[i]["event_id"]
                del stream[i]["event_activity"]
                if "event_timestamp" in stream[i]:
                    del stream[i]["event_timestamp"]
            if include_activity_timest_in_key:
                grouped_stream[(event_id, event_activity, event_timestamp)].append(stream[i])
            else:
                grouped_stream[event_id].append(stream[i])
        i = i + 1

    return grouped_stream
