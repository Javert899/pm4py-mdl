import pandas as pd
from dateutil import parser
from pm4pymdl.objects.mdl.exporter import factory as mdl_exporter
from pm4pymdl.objects.mdl.importer import factory as mdl_importer
import os


def execute_script():
    stream1 = [{"event_id": "1", "event_activity": "A", "event_timestamp": parser.parse("1970-01-01 00:00:00"),
                "order": ["O1"]},
               {"event_id": "2", "event_activity": "B", "event_timestamp": parser.parse("1970-01-01 00:00:00"),
                "order": ["O1"], "item": ["I1, I2"]}]
    df = pd.DataFrame(stream1)
    df.type = "succint"
    stream2 = [{"object_id": "O1", "object_type": "order", "object_buyer": "Alessandro"},
               {"object_id": "I1", "object_type": "item", "object_cost": 600}]
    obj_df = pd.DataFrame(stream2)
    mdl_exporter.apply(df, "prova.mdl", obj_df=obj_df)
    df, obj_df = mdl_importer.apply("prova.mdl", return_obj_dataframe=True)
    mdl_exporter.apply(df, "prova2.mdl", obj_df=obj_df)
    os.remove("prova.mdl")
    os.remove("prova2.mdl")


if __name__ == "__main__":
    execute_script()
