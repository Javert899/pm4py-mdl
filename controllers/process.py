from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4pymdl.algo.mvp.gen_framework import factory as mdfg_disc_factory
from pm4pymdl.algo.mvp.get_logs_and_replay import factory as petri_disc_factory
from pm4pymdl.visualization.mvp.gen_framework import factory as mdfg_vis_factory
from pm4pymdl.visualization.petrinet import factory as pn_vis_factory
from pm4pymdl.algo.mvp.utils import get_activ_attrs_with_type, get_activ_otypes
import base64
import tempfile
from copy import copy
import json

class Process(object):
    def __init__(self, name, mdl_path):
        self.name = name
        self.mdl_path = mdl_path
        self.dataframe = mdl_importer.apply(self.mdl_path)
        try:
            if self.dataframe.type == "succint":
                self.dataframe = succint_mdl_to_exploded_mdl.apply(self.dataframe)
        except:
            pass
        self.session_objects = {}
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

    def get_controller(self, session):
        if not session in self.session_objects:
            self.session_objects[session] = self
        obj = self.session_objects[session]
        activities = list(obj.dataframe["event_activity"].unique())
        activities_attr_types = {}
        activities_object_types = {}
        for act in activities:
            activities_attr_types[act] = get_activ_attrs_with_type.get(obj.dataframe, act)
            activities_object_types[act] = get_activ_otypes.get(obj.dataframe, act)
        cobject = {"activities": activities, "attr_types": activities_attr_types, "obj_types": activities_object_types}
        obj.cobject = json.dumps(cobject)
        obj.get_visualization()
        return obj

    def get_dfg_visualization(self):
        model = mdfg_disc_factory.apply(self.dataframe, model_type_variant=self.selected_model_type)
        gviz = mdfg_vis_factory.apply(model, parameters={"min_acti_count": self.selected_min_acti_count,
                                                         "min_edge_count": self.selected_min_edge_freq_count, "format": "svg"})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        tfilepath.name = "prova.svg"
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')


    def get_petri_visualization(self):
        pass
