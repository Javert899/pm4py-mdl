from pm4pymdl.objects.mdl.importer import factory as mdl_importer

df, obj_df = mdl_importer.apply("../example_logs/mdl/mdl-running-example-w-objects.mdl", return_obj_dataframe=True)
print(df)
print(obj_df)

products = obj_df[obj_df["object_type"] == "products"].dropna(how="all", axis=1)
customers = obj_df[obj_df["object_type"] == "customers"].dropna(how="all", axis=1)

print(products)
print(customers)
