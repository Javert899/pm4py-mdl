from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.get_logs_and_replay import factory as discovery_factory
from pm4pymdl.visualization.petrinet import factory as pn_vis_factory

df = mdl_importer.apply("../example_logs/mdl/order_management.mdl")
model = discovery_factory.apply(df)
gviz = pn_vis_factory.apply(model, parameters={"format": "svg"})
pn_vis_factory.view(gviz)
