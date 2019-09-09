import math
import random
import string
import time
import numpy as np

import pandas as pd


def generate_random_string(N):
    """
    Generate a random string

    Parameters
    ------------
    N
        length of the random string

    Returns
    ------------
    stru
        Random string
    """
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))


def get_time(timestamp):
    """
    Get a string representing the timestamp

    Parameters
    ------------
    timestamp
        Numeric timestamp

    Returns
    ------------
    stru
        String timestamp
    """
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp)).replace(" ", "T") + ".000"


def generate_mvp_log(n_events=1000, n_classes=None, n_activities=None, n_objects_per_class=None):
    """
    Generate a MVP model

    Parameters
    -------------
    n_events
        Number of events
    n_classes
        Number of classes
    n_activities
        Number of activities
    n_objects_per_class
        Number of objects per class

    Returns
    -------------
    log
        MVP log
    """
    if n_classes is None:
        # default number of classes
        n_classes = max(10, math.floor(n_events / 1000))

    if n_objects_per_class is None:
        # default number of objects per class
        n_objects_per_class = math.floor(n_events / (n_classes))

    if n_activities is None:
        # default number of activities
        n_activities = max(10, math.floor(n_events / 10000))

    print("generating model with events=",n_events, "objects=",(n_objects_per_class*n_classes)," classes=",n_classes," activities=",n_activities)

    this_timestamp = 0

    all_activities = []

    for i in range(n_activities):
        all_activities.append("activity_"+str(i))

    classes = []
    objects_per_class = {}

    columns = {"event_id": [], "event_activity": [], "event_timestamp": []}

    for i in range(n_classes):
        this_class = generate_random_string(20)
        classes.append(this_class)
        columns[this_class] = []
        objects_per_class[this_class] = []
        for j in range(n_objects_per_class):
            objects_per_class[this_class].append(this_class + "_" + str(j))

    for i in range(n_events):
        event_name = "event_" + str(i)

        this_timestamp = this_timestamp + 1
        timestamp = get_time(this_timestamp)

        activity = all_activities[random.randrange(0, len(all_activities))]

        prev_idx = len(columns["event_id"])

        #print(classes)

        for j in range(n_classes):
            columns["event_id"].append(event_name)
            columns["event_activity"].append(activity)
            columns["event_timestamp"].append(timestamp)
            for col in classes:
                columns[col].append(np.nan)

        for j in range(n_classes):
            classe = classes[j]
            chosen_object_value = objects_per_class[classe][random.randrange(0,len(objects_per_class[classe]))]

            columns[classe][prev_idx + j] = chosen_object_value
        if i % 10000 == 0 and i > 0:
            print(i,"of",n_events)
    dataframe = pd.DataFrame(data=columns)
    return dataframe
