import sqlite3
from datetime import datetime

import pandas as pd


def apply(file_path, parameters=None):
    """
    Import an OpenSLEX model

    Parameters
    -------------
    file_path
        File path
    parameters
        Possible parameters of the algorithm

    Returns
    -------------
    dataframe
        Dataframe
    """
    if parameters is None:
        parameters = {}

    conn = sqlite3.connect(file_path)
    classes = {}
    object_classes = {}
    object_versions = {}
    activities = {}
    activities_instances = {}
    events = {}
    event_to_objv = {}
    relations = {}

    c = conn.cursor()
    c.execute("SELECT id, name FROM class")
    for result in c.fetchall():
        classes[result[0]] = result[1]
    c.execute("SELECT id, class_id FROM object")
    for result in c.fetchall():
        object_classes[result[0]] = result[1]
    c.execute("SELECT id, name FROM activity")
    for result in c.fetchall():
        activities[result[0]] = result[1]
    c.execute("SELECT id, name FROM activity")
    for result in c.fetchall():
        activities[result[0]] = result[1]
    c.execute("SELECT id, activity_id FROM activity_instance")
    for result in c.fetchall():
        activities_instances[result[0]] = result[1]
    c.execute("SELECT id, object_id FROM object_version")
    for result in c.fetchall():
        object_versions[result[0]] = result[1]
    c.execute("SELECT id, activity_instance_id, timestamp FROM event")
    for result in c.fetchall():
        events[result[0]] = (result[1], result[2])
    c.execute("SELECT event_id, object_version_id FROM event_to_object_version")
    for result in c.fetchall():
        if not result[0] in event_to_objv:
            event_to_objv[result[0]] = []
        event_to_objv[result[0]].append(result[1])
    c.execute("SELECT id, source_object_version_id, target_object_version_id FROM relation")
    for result in c.fetchall():
        if not result[0] in relations:
            relations[result[0]] = []
        relations[result[0]].append(result[1])

    stream = []
    for event in events:
        if event in event_to_objv:
            activity = activities[activities_instances[events[event][0]]]
            timestamp = datetime.utcfromtimestamp(events[event][1] / 1000.0)
            for source_objv in event_to_objv[event]:
                source_obj_id = object_versions[source_objv]
                source_obj_class = classes[object_classes[source_obj_id]]
                stream.append({"event_id": event, "event_activity": activity, "event_timestamp": timestamp,
                               source_obj_class: source_obj_id})
                if source_objv in relations:
                    for target_objv in relations[source_objv]:
                        target_obj_id = object_versions[target_objv]
                        target_obj_class = classes[object_classes[target_obj_id]]
                        stream.append({"event_id": event, "event_activity": activity, "event_timestamp": timestamp,
                                       target_obj_class: target_obj_id})
    dataframe = pd.DataFrame.from_dict(stream)
    dataframe = dataframe.sort_values("event_timestamp")

    dataframe.type = "exploded"

    return dataframe
