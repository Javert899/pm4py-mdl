from copy import copy
from datetime import datetime
from dateutil import parser

import pandas as pd

import random


def apply(file_path, parameters=None):
    """
    Apply the importing of a XOC file

    Parameters
    ------------
    file_path
        Path to the XOC file
    parameters
        Import parameters

    Returns
    ------------
    dataframe
        Dataframe
    """
    if parameters is None:
        parameters = {}

    import_timestamp = parameters["import_timestamp"] if "import_timestamp" in parameters else True
    sample_probability = parameters["sample_probability"] if "sample_probability" in parameters else None
    classes_to_reference = set(parameters["classes_to_reference"]) if "classes_to_reference" in parameters else None
    if classes_to_reference is None:
        classes_to_reference = set()

    F = open(file_path, "r")
    content = F.read()
    F.close()
    events = content.split("<event>")
    stream = []
    stream_strings = []
    i = 1
    considered_events = 0
    considered_objects = set()
    considered_activities = set()
    considered_classes = set()
    while i < len(events) - 1:
        if sample_probability is not None:
            r = random.random()
            if r > sample_probability:
                i = i + 1
                continue
        considered_events = considered_events + 1
        event_id = events[i].split("\"id\" value=\"")[1].split("\"")[0]
        event_activity = events[i].split("\"activity\" value=\"")[1].split("\"")[0]
        considered_activities.add(event_activity)
        event_timestamp0 = events[i].split("\"timestamp\" value=\"")[1].split("\"")[0].replace(" CET", "")
        event_timestamp = None
        if import_timestamp:
            try:
                event_timestamp = parser.parse(event_timestamp0)
            except:
                pass
        if event_timestamp is not None:
            event_dictio = {"event_id": event_id, "event_activity": event_activity, "event_timestamp": event_timestamp}
        else:
            event_dictio = {"event_id": event_id, "event_activity": event_activity}
        references = events[i].split("<references>")[1].split("</references>")[0].split("<object>")
        referenced_classes = set()
        j = 1
        while j < len(references):
            object_class = references[j].split("\"class\" value=\"")[1].split("\"")[0]
            referenced_classes.add(object_class)
            j = j + 1
        if referenced_classes.issuperset(classes_to_reference):
            j = 1
            while j < len(references):
                this_event_dictio = copy(event_dictio)
                object_id = references[j].split("\"id\" value=\"")[1].split("\"")[0]
                considered_objects.add(object_id)
                object_class = references[j].split("\"class\" value=\"")[1].split("\"")[0]
                considered_classes.add(object_class)
                this_event_dictio[object_class] = object_id
                this_event_dictio_stri = str(this_event_dictio)
                if this_event_dictio_stri not in stream_strings:
                    stream.append(this_event_dictio)
                    stream_strings.append(this_event_dictio_stri)
                j = j + 1
        i = i + 1
    dataframe = pd.DataFrame.from_dict(stream)
    if import_timestamp:
        dataframe = dataframe.sort_values(["event_timestamp", "event_id"])

    dataframe.type = "exploded"

    print("events: ",considered_events,"objects: ",len(considered_objects),"activities: ",len(considered_activities),"classes: ",len(considered_classes))
    return dataframe
