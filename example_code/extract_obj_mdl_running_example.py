from pm4pymdl.objects.mdl.importer import importer as mdl_importer
import os


df, obj_df = mdl_importer.apply("../example_logs/mdl/mdl-running-example-w-objects.mdl", return_obj_dataframe=True)
print(df)
print(obj_df)

df.to_csv("only_events.csv", index=False)
obj_df.to_csv("all_objects.csv", index=False)

products = obj_df[obj_df["object_type"] == "products"].dropna(how="all", axis=1)
customers = obj_df[obj_df["object_type"] == "customers"].dropna(how="all", axis=1)

print(products)
print(customers)

products.to_csv("products.csv", index=False)
customers.to_csv("customers.csv", index=False)

os.remove("only_events.csv")
os.remove("all_objects.csv")
os.remove("products.csv")
os.remove("customers.csv")
