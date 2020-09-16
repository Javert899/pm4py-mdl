def calculate_lead_time(succint_dataframe):
    if len(succint_dataframe) > 0:
        ev_timest = succint_dataframe[["event_start_timestamp", "event_timestamp"]].to_dict('r')
        ev_timest = sorted([(x["event_start_timestamp"].timestamp(), x["event_timestamp"].timestamp()) for x in ev_timest])
        return max(x[1] for x in ev_timest) - ev_timest[0][0]
    return 0.0


def calculate_cycle_time(succint_dataframe):
    if len(succint_dataframe) > 0:
        cycle = 0.0
        ev_timest = succint_dataframe[["event_start_timestamp", "event_timestamp"]].to_dict('r')
        ev_timest = sorted(
            [(x["event_start_timestamp"].timestamp(), x["event_timestamp"].timestamp()) for x in ev_timest])
        curr_min = ev_timest[0][0]
        curr_max = ev_timest[0][1]
        i = 1
        while i < len(ev_timest):
            this_min = ev_timest[i][0]
            this_max = ev_timest[i][1]

            if this_min > curr_max:
                cycle += (curr_max - curr_min)
                curr_min = this_min
                curr_max = this_max
            else:
                curr_max = max(curr_max, this_max)
            i = i +1
        cycle += (curr_max - curr_min)
        return cycle
    return 0.0
