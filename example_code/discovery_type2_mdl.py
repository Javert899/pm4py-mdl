from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.gen_framework import factory as discovery
from pm4pymdl.visualization.mvp.gen_framework import factory as vis_factory

df = mdl_importer.apply("../example_logs/mdl/order_management.mdl")
model = discovery.apply(df, model_type_variant="model2", node_freq_variant="type21", edge_freq_variant="type211")
gviz = vis_factory.apply(model)
vis_factory.view(gviz)
