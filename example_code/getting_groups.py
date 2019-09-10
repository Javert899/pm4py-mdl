from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl

# import a succint MDL table
succint_table = mdl_importer.apply("../example_logs/mdl/order_management.mdl")
# convert it into an exploded table
exploded_table = succint_mdl_to_exploded_mdl.apply(succint_table)
# then, get the groups by event_activity and order
table_groups = exploded_table.groupby(["event_activity", "order"])
# get the groups
groups = table_groups.groups

for group in groups:
    specific_exp_table = table_groups.get_group(group)

    print(specific_exp_table)
