from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl, exploded_mdl_to_succint_mdl
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4pymdl.algo.mvp.gen_framework import factory as mdfg_disc_factory
from pm4pymdl.algo.mvp.get_logs_and_replay import factory as petri_disc_factory
from pm4pymdl.visualization.mvp.gen_framework import factory as mdfg_vis_factory
from pm4pymdl.visualization.petrinet import factory as pn_vis_factory
from pm4pymdl.algo.mvp.utils import get_activ_attrs_with_type, get_activ_otypes
from pm4pymdl.algo.mvp.utils import filter_act_attr_val, filter_act_ot, filter_timestamp, filter_metaclass, filter_specific_path
from pm4pymdl.algo.mvp.utils import distr_act_attrname, distr_act_otype, dist_timestamp, clean_objtypes
from pm4pymdl.algo.mvp.discovery import factory as mvp_discovery
from pm4pymdl.visualization.mvp import factory as mvp_vis_factory
from controllers import defaults
import numpy as np
import base64
import tempfile
from copy import copy
import json
import networkx as nx
from networkx.algorithms.community import asyn_lpa_communities
from networkx.algorithms.community import quality
from scipy.linalg.blas import sgemm


class Process(object):
    def __init__(self, name, mdl_path, shared_logs):
        self.shared_logs = shared_logs
        self.shared_logs_names = []
        self.parent = self
        self.act_obj_types = None
        self.initial_act_obj_types = None
        self.activities = []
        self.obj_types = []
        self.stream = None
        self.nodes = None
        self.events_corr = None
        self.matrix = None
        self.powered_matrix = None
        self.graph = None
        self.row_sum = None
        self.overall_sum = 0
        self.default_exponent = 3
        self.selected_act_obj_types = None
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
                                     "petri": "Object-centric Petri net", "mvp_frequency": "MVP frequency",
                                     "mvp_performance": "MVP performance"}
        self.selected_model_type = defaults.DEFAULT_MODEL_TYPE
        self.selected_min_acti_count = 5
        self.selected_min_edge_freq_count = 4
        self.model_view = ""

    def get_stream(self):
        if self.stream is None:
            self.stream = self.dataframe.to_dict('r')
        return self.stream

    def build_nodes(self):
        if self.nodes is None:
            self.get_stream()
            self.nodes = dict()
            self.events_corr = {}
            for ev in self.stream:
                ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
                id = "event_id=" + str(ev2["event_id"])
                activity = "event_activity=" + ev2["event_activity"]
                if id not in self.nodes:
                    self.nodes[id] = len(self.nodes)
                if activity not in self.nodes:
                    self.nodes[activity] = len(self.nodes)
                self.events_corr = {ev2["event_id"]: id}
                for col in ev2:
                    if not col.startswith("event_"):
                        val = ev2[col]
                        oid = "object_id=" + str(val)
                        cla = "class=" + str(col)
                        if oid not in self.nodes:
                            self.nodes[oid] = len(self.nodes)
                        if cla not in self.nodes:
                            self.nodes[cla] = len(self.nodes)
        return self.nodes, self.events_corr

    def build_matrix(self):
        self.build_nodes()
        self.matrix = np.zeros((len(self.nodes), len(self.nodes)), dtype=np.float32)
        for ev in self.stream:
            ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
            id = "event_id=" + str(ev2["event_id"])
            activity = "event_activity=" + ev2["event_activity"]
            self.matrix[self.nodes[id], self.nodes[activity]] = 1
            self.matrix[self.nodes[activity], self.nodes[id]] = 1
            for col in ev2:
                if not col.startswith("event_"):
                    val = ev2[col]
                    self.matrix[self.nodes["object_id=" + str(val)], self.nodes["class=" + str(col)]] = 1
                    self.matrix[self.nodes["class=" + str(col)], self.nodes["object_id=" + str(val)]] = 1
                    self.matrix[self.nodes[id], self.nodes["object_id=" + str(val)]] = 1
                    self.matrix[self.nodes["object_id=" + str(val)], self.nodes[id]] = 1
        self.overall_sum = np.sum(self.matrix)
        self.row_sum = np.sum(self.matrix, axis=1)
        self.matrix = self.matrix / self.row_sum[:, np.newaxis]
        return self.nodes, self.events_corr, self.matrix

    def build_nx_graph(self, threshold=0.03):
        if self.graph is None:
            self.graph = nx.Graph()
            for j in range(self.powered_matrix.shape[0]):
                self.graph.add_node(j)
            for j in range(self.powered_matrix.shape[0]):
                vec = self.powered_matrix[j, :]
                max_vec = max(vec)
                for z in range(self.powered_matrix.shape[1]):
                    if self.powered_matrix[j, z] > max_vec * threshold:
                        self.graph.add_edge(j, z, weight=self.powered_matrix[j, z] * self.row_sum[j] / self.overall_sum)
        return self.graph

    def get_graph(self, exponent=None):
        import time
        if exponent is None:
            exponent = self.default_exponent
        if self.matrix is None:
            self.build_nodes()
            print("1", time.time())
            self.build_matrix()
        if self.powered_matrix is None:
            print("2", time.time())
            self.matrix_power(exponent)
        if self.graph is None:
            print("3", time.time())
            self.build_nx_graph()
        print("4", time.time())
        return self.nodes, self.events_corr, self.matrix, self.graph

    def matrix_power(self, N):
        if self.powered_matrix is None:
            self.powered_matrix = np.linalg.matrix_power(self.matrix, N)
        return self.powered_matrix

    def events_list(self):
        obj = copy(self)
        succint_table = exploded_mdl_to_succint_mdl.apply(obj.dataframe)
        obj.events, obj.columns = self.get_columns_events(succint_table)
        return obj

    def events_list_spec_objt(self, obj_id, obj_type):
        obj = copy(self)
        filtered_df = obj.dataframe[obj.dataframe[obj_type] == obj_id]
        considered_df = filter_metaclass.do_filtering(obj.dataframe, filtered_df)
        succint_table = exploded_mdl_to_succint_mdl.apply(considered_df)
        events, columns = self.get_columns_events(succint_table)
        ret = {"events": events, "columns": columns}
        return ret

    def get_columns_events(self, table):
        columns = [str(x) for x in table.columns]
        columns = sorted([x for x in columns if "Unnamed" not in x])
        event_cols = [x for x in columns if x.startswith("event_")]
        obj_cols = [x for x in columns if not x.startswith("event_")]
        columns = event_cols + obj_cols
        stream = table[columns].to_dict('r')
        events = []
        for ev in stream:
            events.append([])
            for col in columns:
                evval = str(ev[col])
                try:
                    val_evval = eval(evval)
                    evval = "%%" + "%%%".join(val_evval)
                except:
                    evalll = evval.lower()
                    if evalll == "nan" or evalll == "nat":
                        evval = " "
                events[-1].append(evval)
            events[-1] = "@@@".join(events[-1])
        events = "###".join(events)
        events = base64.b64encode(events.encode('utf-8')).decode('utf-8')
        columns = "@@@".join(columns)

        return events, columns

    def get_controller(self, session):
        if not session in self.session_objects:
            self.session_objects[session] = self
        obj = self.session_objects[session]
        obj.set_properties()

        return obj

    def get_act_attr_types(self, activities):
        activities_attr_types = {}
        for act in activities:
            activities_attr_types[act] = [(x, y) for x, y in get_activ_attrs_with_type.get(self.dataframe, act).items()]
        return activities_attr_types

    def get_act_obj_types(self, activities):
        activities_object_types = {}
        for act in activities:
            activities_object_types[act] = get_activ_otypes.get(self.dataframe, act)
        self.act_obj_types = activities_object_types
        if self.initial_act_obj_types is None:
            self.initial_act_obj_types = activities_object_types
        if self.selected_act_obj_types is None:
            self.selected_act_obj_types = self.initial_act_obj_types
        else:
            act_keys = list(self.selected_act_obj_types.keys())
            for key in act_keys:
                if key not in self.act_obj_types:
                    del self.selected_act_obj_types[key]
                else:
                    obj_keys = list(self.selected_act_obj_types[key])
                    for ot in obj_keys:
                        if ot not in self.act_obj_types[key]:
                            del self.selected_act_obj_types[key][self.selected_act_obj_types[key].index(ot)]
        return activities_object_types

    def set_properties(self):
        self.stream = None
        self.nodes = None
        self.events_corr = None
        self.matrix = None
        self.row_sum = None
        self.overall_sum = 0
        self.powered_matrix = None
        self.graph = None
        self.get_graph()
        self.activities = sorted(list(self.dataframe["event_activity"].unique()))
        attr_types = self.get_act_attr_types(self.activities)
        act_obj_types = self.get_act_obj_types(self.activities)
        self.obj_types = set()
        for act in act_obj_types:
            self.obj_types = self.obj_types.union(set(act_obj_types[act]))
        self.obj_types = sorted(list(self.obj_types))

        cobject = {"activities": self.activities, "attr_types": attr_types, "obj_types": act_obj_types,
                   "all_otypes": self.obj_types, "act_obj_types": self.act_obj_types,
                   "selected_act_obj_types": self.selected_act_obj_types}
        self.cobject = base64.b64encode(json.dumps(cobject).encode('utf-8')).decode('utf-8')
        self.dataframe_length = len(self.dataframe)

    def get_names(self):
        self.shared_logs_names = "@@@".join([x for x in self.shared_logs])
        return self

    def get_visualization(self, min_acti_count=0, min_paths_count=0, model_type=defaults.DEFAULT_MODEL_TYPE):
        obj = copy(self)
        obj.get_names()
        obj.selected_min_acti_count = min_acti_count
        obj.selected_min_edge_freq_count = min_paths_count
        obj.selected_model_type = model_type
        reversed_dict = {}
        for act in obj.activities:
            if act in obj.selected_act_obj_types:
                for ot in obj.obj_types:
                    if ot in obj.selected_act_obj_types[act]:
                        if not ot in reversed_dict:
                            reversed_dict[ot] = []
                        reversed_dict[ot].append(act)

        param = {}
        param["allowed_activities"] = reversed_dict
        obj.dataframe = clean_objtypes.perfom_cleaning(obj.dataframe, parameters=param)
        obj.dataframe = obj.dataframe.dropna(how="all", axis=1)

        if obj.selected_model_type.startswith("model"):
            obj.get_dfg_visualization()
        elif obj.selected_model_type.startswith("petri"):
            obj.get_petri_visualization()
        else:
            obj.get_mvp_visualization()
        return obj

    def get_dfg_visualization(self):
        if self.selected_model_type == "model1":
            model = mdfg_disc_factory.apply(self.dataframe, model_type_variant=self.selected_model_type,
                                            node_freq_variant="type1", edge_freq_variant="type11")
        elif self.selected_model_type == "model2":
            model = mdfg_disc_factory.apply(self.dataframe, model_type_variant=self.selected_model_type,
                                            node_freq_variant="type21", edge_freq_variant="type211")
        elif self.selected_model_type == "model3":
            model = mdfg_disc_factory.apply(self.dataframe, model_type_variant=self.selected_model_type,
                                            node_freq_variant="type31", edge_freq_variant="type11")
        gviz = mdfg_vis_factory.apply(model, parameters={"min_node_freq": self.selected_min_acti_count,
                                                         "min_edge_freq": self.selected_min_edge_freq_count,
                                                         "format": "svg"})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def get_petri_visualization(self):
        model = petri_disc_factory.apply(self.dataframe, parameters={"min_node_freq": self.selected_min_acti_count,
                                                                     "min_edge_freq": self.selected_min_edge_freq_count})
        gviz = pn_vis_factory.apply(model, parameters={"format": "svg"})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def get_mvp_visualization(self):
        parameters = {}
        parameters["format"] = "svg"
        parameters["min_act_count"] = self.selected_min_acti_count
        parameters["min_dfg_occurrences"] = self.selected_min_edge_freq_count
        if self.selected_model_type == "mvp_performance":
            parameters["performance"] = True
        else:
            parameters["performance"] = False
        model = mvp_discovery.apply(self.dataframe, parameters=parameters)
        gviz = mvp_vis_factory.apply(model, parameters=parameters)
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mvp_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def apply_spec_path_filter(self, session, obytype, act1, act2):
        obj = copy(self.session_objects[session])
        obj.dataframe = filter_specific_path.apply(obj.dataframe, obytype, act1, act2)
        obj.set_properties()
        self.session_objects[session] = obj

    def apply_activity_filter(self, session, activity, positive):
        obj = copy(self.session_objects[session])
        fd0 = obj.dataframe[obj.dataframe["event_activity"] == activity]
        if positive == "1":
            obj.dataframe = filter_metaclass.do_filtering(obj.dataframe, fd0)
        else:
            obj.dataframe = filter_metaclass.do_negative_filtering(obj.dataframe, fd0)
        obj.set_properties()
        self.session_objects[session] = obj

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

    def apply_timestamp_filter(self, session, dt1, dt2):
        obj = copy(self.session_objects[session])
        obj.dataframe = filter_timestamp.apply(obj.dataframe, dt1, dt2)
        obj.set_properties()
        self.session_objects[session] = obj

    def get_float_attr_summary(self, session, activity, attr_name):
        return distr_act_attrname.get(self.session_objects[session].dataframe, activity, attr_name)

    def get_timestamp_summary(self, session):
        return dist_timestamp.get(self.session_objects[session].dataframe)

    def get_ot_distr_summary(self, session, activity, ot):
        return distr_act_otype.get(self.session_objects[session].dataframe, activity, ot)

    def reset_filters(self, session):
        self.session_objects[session] = self.parent
        self.session_objects[session].selected_act_obj_types = self.session_objects[session].initial_act_obj_types
