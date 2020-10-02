from pm4pymdl.objects.xoc.importer import importer as xoc_importer
from pm4pymdl.algo.mvp.gen_framework import algorithm as discovery
from pm4pymdl.visualization.mvp.gen_framework import visualizer as vis_factory

df = xoc_importer.apply("../example_logs/xoc/log.xoc")
model = discovery.apply(df, model_type_variant="model1", node_freq_variant="type1", edge_freq_variant="type11")
gviz = vis_factory.apply(model, parameters={"format": "svg"})
vis_factory.view(gviz)
