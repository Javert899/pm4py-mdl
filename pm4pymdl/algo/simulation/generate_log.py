import uuid
import random
from datetime import datetime
import numpy as np
from copy import copy


def generate_stream(n_obj=10000, n_eve=100000, n_classes=3, n_activities=40, avg_obj_per_class=1.0):
    activities = []
    for i in range(n_activities):
        activities.append(str(uuid.uuid4()))
    classes_objects = {}
    for i in range(n_classes):
        classes_objects[str(uuid.uuid4())] = list()
    classes = list(classes_objects.keys())
    for i in range(n_obj):
        cl = random.choice(classes)
        classes_objects[cl].append(str(uuid.uuid4()))
    timestamp = 10000000
    discovery_stream = []
    conformance_stream = []
    for i in range(n_eve):
        basic_eve = {"event_id": str(i), "event_activity": random.choice(activities), "event_timestamp": datetime.fromtimestamp(timestamp)}
        eve = copy(basic_eve)
        for cl in classes:
            num_obj = max(min(int(round(avg_obj_per_class - 1 + np.random.exponential())), int(round(len(classes_objects[cl])/2))), 1)
            objs = list(random.sample(classes_objects[cl], num_obj))
            eve[cl] = objs
            for o in objs:
                red_eve = copy(basic_eve)
                red_eve[cl] = o
                discovery_stream.append(red_eve)
        conformance_stream.append(eve)
        timestamp = timestamp + 1
    return discovery_stream, conformance_stream

