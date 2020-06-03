from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4pymdl.objects.mdl.exporter import factory as mdl_exporter
import random
import pandas as pd

succint_df = mdl_importer.apply("example_logs/mdl/mdl-running-example.mdl")
df = succint_mdl_to_exploded_mdl.apply(succint_df)
products = df["products"].dropna().unique()
customers = df["customers"].dropna().unique()

objects = []
for p in products:
    objects.append({"object_id": p, "object_type": "products", "object_cost": random.randrange(100, 500), "object_producer": random.choice(["A", "B", "C"])})
for c in customers:
    objects.append({"object_id": c, "object_type": "customers", "object_age": random.randrange(30, 60), "object_bankaccount": random.randrange(1000, 100000)})

print(objects)

obj_df = pd.DataFrame(objects)
mdl_exporter.apply(df, "mdl-running-example-w-objects.mdl", obj_df=obj_df)
