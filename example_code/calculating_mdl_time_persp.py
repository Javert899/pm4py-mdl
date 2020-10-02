from pm4pymdl.objects.mdl.importer import importer as mdl_importer

# import a log in Parquet format, that is stored as exploded MDL table
exploded_table = mdl_importer.apply("../example_logs/parquet/pkdd99.parquet")
print(exploded_table.columns)

# focus on the account ID: we wish to calculate the time from the previous event
exploded_table = exploded_table.dropna(subset=["account_id"], how="any")
exploded_table_prev_ev = exploded_table.copy()
exploded_table_prev_ev["@@index"] = exploded_table_prev_ev.index
exploded_table_prev_ev = exploded_table_prev_ev.reset_index()
exploded_table_prev_ev = exploded_table_prev_ev.sort_values(["account_id", "event_timestamp", "@@index"])
shifted_table = exploded_table_prev_ev.shift()
exploded_table_prev_ev["@@time_prev"] = (
            exploded_table_prev_ev["event_timestamp"] - shifted_table["event_timestamp"]).astype('timedelta64[s]')
exploded_table_prev_ev = exploded_table_prev_ev[exploded_table_prev_ev["account_id"] == shifted_table["account_id"]]

exploded_table_prev_ev = exploded_table_prev_ev.dropna(subset=["@@time_prev"])
exploded_table_prev_ev = exploded_table_prev_ev.sort_values(["@@time_prev", "event_timestamp", "@@index"])

# keep only the lowest difference between times
exploded_table_prev_ev = exploded_table_prev_ev.groupby("event_id").first().reset_index()

print(exploded_table_prev_ev)
# focus on the account_id
