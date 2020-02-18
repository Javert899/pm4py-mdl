from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4pymdl.algo.mvp.gen_framework import factory as mdfg_disc_factory
from pm4pymdl.algo.mvp.get_logs_and_replay import factory as petri_disc_factory
from pm4pymdl.visualization.mvp.gen_framework import factory as mdfg_vis_factory
from pm4pymdl.visualization.petrinet import factory as pn_vis_factory
import base64
import tempfile


class Process(object):
    def __init__(self, name, mdl_path):
        self.name = name
        self.mdl_path = mdl_path
        self.original_dataframe = mdl_importer.apply(self.mdl_path)
        try:
            if self.original_dataframe.type == "succint":
                self.original_dataframe = succint_mdl_to_exploded_mdl.apply(self.original_dataframe)
        except:
            pass
        self.filtered_dataframe = self.original_dataframe
        self.possible_model_types = {"model1": "mDFGs type 1", "model2": "mDFGs type 2", "model3": "mDFGs type 3",
                                     "petri": "Object-centric Petri net"}
        self.selected_model_type = "model1"
        self.selected_min_acti_count = 0
        self.selected_min_edge_freq_count = 0
        self.model_view = ""
        self.get_visualization()

    def get_visualization(self):
        if self.selected_model_type.startswith("model"):
            self.get_dfg_visualization()
        else:
            self.get_petri_visualization()

    def get_dfg_visualization(self):
        model = mdfg_disc_factory.apply(self.filtered_dataframe, model_type_variant=self.selected_model_type)
        gviz = mdfg_vis_factory.apply(model, parameters={"min_acti_count": self.selected_min_acti_count,
                                                         "min_edge_count": self.selected_min_edge_freq_count, "format": "svg"})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read())


    def get_petri_visualization(self):
        pass
