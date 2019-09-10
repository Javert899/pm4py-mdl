from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4pymdl.algo.mvp.gen_framework import factory as discovery
from pm4pymdl.visualization.mvp.gen_framework import factory as vis_factory
from pm4pymdl.objects.mdl.exporter import factory as mdl_exporter

# import a succint MDL table
succint_table = mdl_importer.apply("example_logs/mdl/order_management.mdl")
print(len(succint_table), succint_table.type)
# convert it into an exploded MDL table
exploded_table = succint_mdl_to_exploded_mdl.apply(succint_table)
print(len(exploded_table), exploded_table.type)
# keeps only events related to orders that have a profit >= 200
# to make the filtering on the exploded table we have to follow the procedure:
f0 = exploded_table[exploded_table["event_profit"] >= 200]
f1 = exploded_table[exploded_table["order"].isin(f0["order"])]
filtered_exploded_table = exploded_table[exploded_table["event_id"].isin(f1["event_id"])]

# suppose that we want to get also the packages related to the filtered orders, then:
f2 = exploded_table[exploded_table["package"].isin(filtered_exploded_table["package"])]
filtered_table_2 = exploded_table[
    exploded_table["event_id"].isin(filtered_exploded_table["event_id"]) | exploded_table["event_id"].isin(
        f2["event_id"])]

# mine a process model out of the filtered table
model = discovery.apply(filtered_table_2)
gviz = vis_factory.apply(model)
vis_factory.view(gviz)

# export the filtered version
mdl_exporter.apply(filtered_table_2, "filtered.mdl")
