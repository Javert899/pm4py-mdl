from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4pymdl.algo.mvp.gen_framework import factory as mdfg_disc_factory
from pm4pymdl.algo.mvp.get_logs_and_replay import factory as petri_disc_factory
from pm4pymdl.visualization.mvp.gen_framework import factory as mdfg_vis_factory
from pm4pymdl.visualization.petrinet import factory as pn_vis_factory
from pm4pymdl.algo.mvp.utils import get_activ_attrs_with_type, get_activ_otypes
from pm4pymdl.algo.mvp.utils import filter_act_attr_val, filter_act_ot
from pm4pymdl.algo.mvp.utils import distr_act_attrname, distr_act_otype
import base64
import tempfile
from copy import copy
import json

class Process(object):
    def __init__(self, name, mdl_path):
        self.parent = self
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
        obj.set_properties()

        return obj

    def set_properties(self):
        activities = list(self.dataframe["event_activity"].unique())
        activities_attr_types = {}
        activities_object_types = {}
        for act in activities:
            activities_attr_types[act] = [(x, y) for x, y in get_activ_attrs_with_type.get(self.dataframe, act).items()]
            activities_object_types[act] = get_activ_otypes.get(self.dataframe, act)
        cobject = {"activities": activities, "attr_types": activities_attr_types, "obj_types": activities_object_types}
        self.cobject = base64.b64encode(json.dumps(cobject).encode('utf-8')).decode('utf-8')
        self.dataframe_length = len(self.dataframe)
        self.get_visualization()

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


    def apply_float_filter(self, session, activity, attr_name, v1, v2):
        obj = copy(self.session_objects[session])
        obj.dataframe = filter_act_attr_val.filter_float(obj.dataframe, activity, attr_name, v1, v2)
        obj.set_properties()
        self.session_objects[session] = obj


    def apply_ot_filter(self, session, activity, ot, v1, v2):
        obj = copy(self.session_objects[session])
        obj.dataframe = filter_act_ot.filter_ot(self.dataframe.copy(), activity, ot, v1, v2)
        obj.set_properties()
        self.session_objects[session] = obj


    def get_float_attr_summary(self, session, activity, attr_name):
        print(activity)
        print(attr_name)
        return distr_act_attrname.get(self.session_objects[session].dataframe, activity, attr_name)


    def get_ot_distr_summary(self, session, activity, ot):
        return distr_act_otype.get(self.session_objects[session].dataframe, activity, ot)


    def reset_filters(self, session):
        self.session_objects[session] = self.parent
