from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.discovery import factory as mvp_disc_factory
from pm4pymdl.visualization.mvp import factory as mvp_vis_factory

df = mdl_importer.apply("../example_logs/mdl/order_management.mdl")
model = mvp_disc_factory.apply(df)
gviz = mvp_vis_factory.apply(model, parameters={"format": "svg"})
mvp_vis_factory.view(gviz)
