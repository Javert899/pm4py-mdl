from pm4pymdl.algo.mvp.utils import exploded_mdl_to_grouped_stream_old

def apply(df, dest_path, parameters=None):
    """
    Export into a XOC format

    Parameters
    ------------
    df
        Dataframe
    dest_path
        Destination path
    parameters
        Parameters of the algorithm
    """
    if parameters is None:
        parameters = {}

    grouped_stream = exploded_mdl_to_grouped_stream_old.apply(df)

    F = open(dest_path, "w")
    F.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n")
    F.write("<log xes.features=\"nested-attributes\">\n")
    for eve_id in grouped_stream:
        first = grouped_stream[eve_id][0]
        F.write("\t<event>\n")
        F.write("\t\t<string key=\"id\" value=\"" + str(eve_id) + "\" />\n")
        F.write("\t\t<string key=\"activity\" value=\"" + first["event_activity"] + "\" />\n")
        F.write("\t\t<string key=\"timestamp\" value=\"" + first["event_timestamp"].strftime(
            "%Y-%m-%d %H:%M:%S") + "\" />\n")
        F.write("\t\t<references>\n")
        for obj in grouped_stream[eve_id]:
            obj_id = None
            obj_class = None
            for prop in obj.keys():
                if not prop.startswith("event_"):
                    obj_class = prop
                    obj_id = obj[prop]
            F.write("\t\t\t<object>\n")
            F.write("\t\t\t\t<string key=\"id\" value=\""+str(obj_id)+"\" />\n")
            F.write("\t\t\t\t<string key=\"class\" value=\""+obj_class+"\" />\n")
            F.write("\t\t\t</object>\n")
        F.write("\t\t</references>\n")
        F.write("\t\t<model>\n")
        F.write("\t\t\t<objects>\n")
        for obj in grouped_stream[eve_id]:
            obj_id = None
            obj_class = None
            for prop in obj.keys():
                if not prop.startswith("event_"):
                    obj_class = prop
                    obj_id = obj[prop]
            F.write("\t\t\t\t<object>\n")
            F.write("\t\t\t\t\t<string key=\"id\" value=\""+str(obj_id)+"\" />\n")
            F.write("\t\t\t\t\t<string key=\"class\" value=\""+obj_class+"\" />\n")
            F.write("\t\t\t\t</object>\n")
        F.write("\t\t\t</objects>\n")
        F.write("\t\t</model>\n")
        F.write("\t</event>\n")
        pass
    F.write("</log>\n")
    F.close()
