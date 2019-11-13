from datetime import datetime
from copy import deepcopy
from random import randrange, choice
import pandas as pd
from collections import Counter
import random

class shared:
    timestamp = 10000000
    # create N orders and deliveries, with 4*N packages
    N = 100
    event_count = 0
    # deliveries hesite
    esito = ["deliver", "undeliver"]


def generate_event(activity, related_classes):
    shared.event_count = shared.event_count + 1

    shared.timestamp = shared.timestamp + 1

    this_timestamp = datetime.fromtimestamp(shared.timestamp)

    base_event = {"event_id": "event_" + str(shared.event_count), "event_activity": activity,
                  "event_timestamp": this_timestamp}

    ret = []
    for cl in related_classes.keys():
        for el in related_classes[cl]:
            this_event = deepcopy(base_event)
            if cl == "order":
                this_event[cl] = "order_" + str(el)
            if cl == "package":
                this_event[cl] = "package_" + str(el)
            if cl == "delivery":
                this_event[cl] = "delivery_" + str(el)
            if cl == "back":
                this_event[cl] = "back_" + str(el)

            ret.append(this_event)

    return ret


def generate_log():
    list_events = []

    for d in range(shared.N*2):

        income = randrange(100, 1000)
        costs = randrange(0, income)
        profit = income - costs

        event = generate_event("create", {"order": [d]})

        for i in range(len(event)):
            event[i]["event_income"] = income
            event[i]["event_costs"] = costs
            event[i]["event_profit"] = profit

        list_events = list_events + event

    i = 0
    while i < shared.N*2:

        list_events = list_events + generate_event("split", {"package": [8*i, 8*i +1, 8*i +2, 8*i +3], "order": [i]})
        list_events = list_events + generate_event("split", {"package": [8*i + 4, 8*i +5, 8*i +6, 8*i +7], "order": [i+1]})

        i = i + 2

    i = 0
    while i < shared.N*2:
        list_events = list_events + generate_event("load", {"package": [8*i, 8*i +1, 8*i +4, 8*i +5], "delivery": [i]})
        list_events = list_events + generate_event("load", {"package": [8*i +2, 8*i +3, 8*i +6, 8*i +7], "delivery": [i+1]})

        i = i + 2

    i = 0
    while i < shared.N*2:
        j = 0
        while True:
            esito = choice(shared.esito)
            j = j + 1

            list_events = list_events + generate_event(esito, {"package": [8*i, 8*i +1, 8*i +4, 8*i +5], "delivery": [i]})
            list_events = list_events + generate_event(esito, {"package": [8*i +2, 8*i +3, 8*i +6, 8*i +7], "delivery": [i+1]})

            list_events = list_events + generate_event("next",
                                                       {
                                                        "delivery": [i]})
            list_events = list_events + generate_event("next",
                                                       {
                                                        "delivery": [i + 1]})

            if not esito == "deliver":
                r = random.random()
                if r < 0.5:
                    list_events = list_events + generate_event("retry",
                                                               {"package": [8 * i, 8 * i + 1, 8 * i + 4, 8 * i + 5],
                                                                "delivery": [i]})
                    list_events = list_events + generate_event("retry",
                                                               {"package": [8 * i + 2, 8 * i + 3, 8 * i + 6, 8 * i + 7],
                                                                "delivery": [i + 1]})
                    continue
                else:
                    list_events = list_events + generate_event("return", {"package": [8 * i, 8 * i + 1, 8 * i + 4, 8 * i + 5, 8*i +2, 8*i +3, 8*i +6, 8*i +7]})
                    break
            else:
                break

        i = i + 2

    for d in range(shared.N*2):

        list_events = list_events + generate_event("notify", {"order": [d]})

    i = 0
    while i < shared.N*2:
        additional_fee = randrange(0, 200)
        event = generate_event("bill", {"package": [8*i, 8*i +1, 8*i +2, 8*i +3], "order": [i]})
        for j in range(len(event)):
            event[j]["event_add_fee"] = additional_fee
        list_events = list_events + event

        event = generate_event("bill", {"package": [8*i + 4, 8*i +5, 8*i +6, 8*i +7], "order": [i+1]})
        for j in range(len(event)):
            event[j]["event_add_fee"] = additional_fee
        list_events = list_events + event

        i = i + 2

    i = 0
    while i < shared.N*2:
        list_events = list_events + generate_event("finish", {"package": [8*i, 8*i +1, 8*i +4, 8*i +5], "delivery": [i]})
        list_events = list_events + generate_event("finish", {"package": [8*i +2, 8*i +3, 8*i +6, 8*i +7], "delivery": [i+1]})

        i = i + 2

    df = pd.DataFrame(list_events)
    df.type = "exploded"

    return df


if __name__ == "__main__":
    df = generate_log()

    print(df)
