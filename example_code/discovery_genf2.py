from pm4pymdl.objects.mdl.importer import importer as mdl_importer
from pm4pymdl.algo.mvp.gen_framework2 import algorithm as mvp_disc_factory
from pm4pymdl.visualization.mvp.gen_framework2 import visualizer as mvp_vis_factory

df = mdl_importer.apply("../example_logs/mdl/order_management.mdl")
classifier_function = lambda x: x["event_activity"] + "+" + x["event_objtype"]
model = mvp_disc_factory.apply(df, classifier_function=classifier_function)
gviz = mvp_vis_factory.apply(model, classifier="aaaa")
mvp_vis_factory.view(gviz)
