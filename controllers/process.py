from pm4pymdl.objects.mdl.importer import importer as mdl_importer
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl, exploded_mdl_to_succint_mdl
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4pymdl.algo.mvp.gen_framework import algorithm as mdfg_disc_factory
from pm4pymdl.algo.mvp.gen_framework2 import algorithm as mdfg_disc_factory2
from pm4pymdl.algo.mvp.gen_framework3 import discovery as mdfg_disc_factory3
from pm4pymdl.algo.mvp.get_logs_and_replay import algorithm as petri_disc_factory
from pm4pymdl.visualization.mvp.gen_framework import visualizer as mdfg_vis_factory
from pm4pymdl.visualization.mvp.gen_framework2 import visualizer as mdfg_vis_factory2
from pm4pymdl.visualization.mvp.gen_framework3 import visualizer as mdfg_vis_factory3
from pm4pymdl.visualization.petrinet import visualizer as pn_vis_factory
from pm4pymdl.algo.mvp.utils import get_activ_attrs_with_type, get_activ_otypes
from pm4pymdl.algo.mvp.utils import filter_act_attr_val, filter_act_ot, filter_timestamp, filter_metaclass, \
    filter_specific_path
from pm4pymdl.algo.mvp.utils import distr_act_attrname, distr_act_otype, dist_timestamp, clean_objtypes
from pm4pymdl.algo.mvp.discovery import algorithm as mvp_discovery
from pm4pymdl.visualization.mvp import visualizer as mvp_vis_factory
from controllers import defaults
import numpy as np
import pandas as pd
import base64
import tempfile
from copy import copy, deepcopy
import json
import networkx as nx
from networkx.algorithms.community import asyn_lpa_communities, greedy_modularity_communities
from networkx.algorithms.community import quality
from scipy.linalg.blas import sgemm
from scipy import spatial
from pm4py.objects.log.log import EventStream
from pm4py.objects.conversion.log import converter as log_conv_factory
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.util import sorting
from collections import Counter

DEFAULT_EXPONENT = 9


class Process(object):
    def __init__(self, name, mdl_path, shared_logs):
        self.shared_logs = shared_logs
        self.shared_logs_names = []
        self.parent = self
        self.obj_types_str = None
        self.act_obj_types = None
        self.initial_act_obj_types = None
        self.activities = []
        self.obj_types = []
        self.clusters = {}
        self.clustersrepr = ""
        self.clusterid = str(id(self))
        self.stream = None
        self.nodes = None
        self.events_corr = None
        self.events_corr2 = None
        self.matrix = None
        self.powered_matrix = None
        self.powered_matrix_2 = None
        self.graph = None
        self.row_sum = None
        self.overall_sum = 0
        self.selected_act_obj_types = None
        self.name = name
        self.mdl_path = mdl_path
        self.succint_dataframe = mdl_importer.apply(self.mdl_path)
        self.succint_dataframe = self.succint_dataframe.dropna(subset=["event_activity"])
        self.succint_dataframe.type = "succint"
        self.exploded_dataframe = succint_mdl_to_exploded_mdl.apply(self.succint_dataframe)
        self.exploded_dataframe.type = "exploded"
        self.session_objects = {}

        self.possible_model_types = {"mvp_frequency": "MVP (frequency)", "mvp_performance": "MVP (performance)",
                                     "process_tree": "oc-PTree", "petri_alpha": "oc-Net-Alpha",
                                     "petri_inductive": "oc-Net-Inductive", "dfg": "oc-DFG", "multigraph": "OC Multigraph"}
        self.selected_model_type = defaults.DEFAULT_MODEL_TYPE
        self.possible_classifiers = {"activity", "combined"}
        self.selected_classifier = "activity"
        self.selected_aggregation_measure = "events"
        self.selected_decoration_measure = "frequency"
        self.selected_projection = "no"
        self.selected_min_acti_count = 800
        self.selected_min_edge_freq_count = 800
        self.epsilon = 0.0
        self.noise_threshold = 0.0
        self.model_view = ""

    def get_stream(self):
        if self.stream is None:
            self.stream = self.exploded_dataframe.to_dict('r')
        return self.stream

    def get_log_obj_type(self, objtype):
        columns = [x for x in self.exploded_dataframe.columns if x.startswith("event_")] + [objtype]
        dataframe = self.exploded_dataframe[columns].dropna(how="any", subset=[objtype])
        dataframe = succint_mdl_to_exploded_mdl.apply(dataframe)
        dataframe = dataframe.rename(columns={"event_activity": "concept:name", "event_timestamp": "time:timestamp",
                                              objtype: "case:concept:name"})
        stream = EventStream(dataframe.to_dict('r'))
        log = log_conv_factory.apply(stream)
        log = sorting.sort_timestamp(log, "time:timestamp")
        exported_log = base64.b64encode(xes_exporter.export_log_as_string(log)).decode("utf-8")
        return self.name + "_" + objtype, "xes", exported_log

    def get_most_similar(self, id, exponent=None):
        if exponent is None:
            exponent = DEFAULT_EXPONENT
        if self.matrix is None:
            self.build_matrix()
        if exponent != DEFAULT_EXPONENT or self.powered_matrix is None:
            self.matrix_power(exponent)
        cid = self.events_corr[id]
        j = self.nodes[cid]
        idxs = []
        jvec = list(self.powered_matrix[j, :] * self.row_sum[j] / self.overall_sum)
        for i in range(self.powered_matrix.shape[0]):
            if not i == j:
                if self.nodes_inv[i].startswith("event_id="):
                    ivec = list(self.powered_matrix[i, :] * self.row_sum[i] / self.overall_sum)
                    idxs.append({"event_id": self.events_corr_inv[self.nodes_inv[i]],
                                 "@@distance": spatial.distance.cosine(jvec, ivec)})
        dataframe = self.exploded_dataframe[self.exploded_dataframe["event_id"].isin([x["event_id"] for x in idxs])]
        dataframe = dataframe.set_index('event_id')
        dataframe2 = pd.DataFrame(idxs).set_index('event_id')
        dataframe["event_ZZdistance"] = dataframe2["@@distance"]
        dataframe = dataframe.reset_index()
        dataframe.type = "exploded"
        succint_table = self.succint_dataframe.sort_values("event_ZZdistance", ascending=True)
        events, columns = self.get_columns_events(succint_table)
        ret = {"events": events, "columns": columns}
        return ret

    def build_nodes(self):
        if self.nodes is None:
            self.get_stream()
            self.nodes = dict()
            self.nodes_inv = dict()
            self.events_corr = {}
            self.events_corr2 = {}
            self.events_corr_inv = {}
            for ev in self.stream:
                ev2 = {x: y for x, y in ev.items() if str(y) != "nan"}
                id = "event_id=" + str(ev2["event_id"])
                activity = "event_activity=" + ev2["event_activity"]
                if id not in self.nodes:
                    self.nodes[id] = len(self.nodes)
                if activity not in self.nodes:
                    self.nodes[activity] = len(self.nodes)
                self.events_corr[ev2["event_id"]] = id
                self.events_corr2[ev2["event_id"]] = ev
                self.events_corr_inv[id] = ev2["event_id"]
                for col in ev2:
                    if not col.startswith("event_"):
                        val = ev2[col]
                        oid = "object_id=" + str(val)
                        cla = "class=" + str(col)
                        if oid not in self.nodes:
                            self.nodes[oid] = len(self.nodes)
                        if cla not in self.nodes:
                            self.nodes[cla] = len(self.nodes)
        for k in self.nodes:
            self.nodes_inv[self.nodes[k]] = k
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
                    self.matrix[self.nodes["object_id=" + str(val)], self.nodes["class=" + str(col)]] += 1
                    self.matrix[self.nodes["class=" + str(col)], self.nodes["object_id=" + str(val)]] += 1
                    self.matrix[self.nodes[id], self.nodes["object_id=" + str(val)]] = 1
                    self.matrix[self.nodes["object_id=" + str(val)], self.nodes[id]] = 1
        self.overall_sum = np.sum(self.matrix)
        self.row_sum = np.sum(self.matrix, axis=1)
        self.matrix = self.matrix / self.row_sum[:, np.newaxis]
        return self.nodes, self.events_corr, self.matrix

    def fill_string(self, stru):
        if len(stru) < 2:
            stru = "0" + stru
        return stru

    def do_clustering(self):
        if len(self.clusters) == 0 or self.clusterid != str(id(self)):
            self.clusterid = str(id(self))
            self.build_nx_graph()
            clusters = list(asyn_lpa_communities(self.graph, weight="weight"))
            new_clusters = []
            for c in clusters:
                new_clusters.append([])
                for el in c:
                    if self.nodes_inv[el].startswith("event_id="):
                        new_clusters[-1].append(self.events_corr_inv[self.nodes_inv[el]])
                if len(new_clusters[-1]) == 0:
                    del new_clusters[-1]
            new_clusters = sorted(new_clusters, key=lambda x: len(x), reverse=True)
            self.clusters = {}
            for i in range(len(new_clusters)):
                self.clusters["Cluster " + self.fill_string(str(i + 1)) + " (" + str(len(new_clusters[i])) + ")"] = \
                    new_clusters[i]
            clusters_keys = sorted(list(self.clusters.keys()))
            self.clustersrepr = "@@@".join(clusters_keys)

    def build_nx_graph(self):
        if self.graph is None:
            self.graph = nx.convert_matrix.from_numpy_matrix(self.powered_matrix)
        return self.graph

    def get_full_matrix(self, exponent=None):
        import time
        if exponent is None:
            exponent = DEFAULT_EXPONENT
        if self.matrix is None:
            self.build_nodes()
            self.build_matrix()
        if self.powered_matrix is None:
            self.matrix_power(exponent)
        return self.nodes, self.events_corr, self.matrix, self.powered_matrix

    def get_graph(self, exponent=None):
        import time
        self.get_full_matrix(exponent=exponent)
        if self.graph is None:
            self.build_nx_graph()
        return self.nodes, self.events_corr, self.matrix, self.powered_matrix, self.graph

    def matrix_power(self, N):
        if self.powered_matrix is None:
            self.powered_matrix = np.linalg.matrix_power(self.matrix, N)
            self.powered_matrix_2 = np.matmul(self.powered_matrix,
                                              np.diag(list(self.row_sum[j] / self.overall_sum for j in
                                                           range(self.powered_matrix.shape[0]))))
        return self.powered_matrix

    def events_list(self, dataframe=None):
        obj = copy(self)
        if dataframe is None:
            dataframe = obj.exploded_dataframe
        obj.events, obj.columns = self.get_columns_events(self.succint_dataframe)
        return obj

    def events_list_spec_objt(self, obj_id, obj_type):
        obj = copy(self)
        filtered_df = obj.exploded_dataframe[obj.exploded_dataframe[obj_type] == obj_id]
        considered_df = filter_metaclass.do_filtering(obj.exploded_dataframe, filtered_df)
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
            activities_attr_types[act] = [(x, y) for x, y in get_activ_attrs_with_type.get(self.exploded_dataframe, act).items()]
        return activities_attr_types

    def get_act_obj_types(self, activities):
        activities_object_types = {}
        for act in activities:
            activities_object_types[act] = get_activ_otypes.get(self.exploded_dataframe, act)
        self.act_obj_types = activities_object_types
        self.obj_types_str = "@@@".join([x for x in self.exploded_dataframe.columns if not x.startswith("event_")])
        if self.initial_act_obj_types is None:
            self.initial_act_obj_types = deepcopy(activities_object_types)
        if self.selected_act_obj_types is None:
            self.selected_act_obj_types = deepcopy(self.initial_act_obj_types)
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

    def reset_properties(self):
        self.stream = None
        self.nodes = None
        self.events_corr = None
        self.events_corr2 = None
        self.matrix = None
        self.row_sum = None
        self.overall_sum = 0
        self.powered_matrix = None
        self.powered_matrix_2 = None
        self.graph = None

    def set_properties(self):
        self.activities = sorted(list(self.exploded_dataframe["event_activity"].unique()))
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
        self.dataframe_length = self.exploded_dataframe["event_id"].nunique()

    def get_names(self):
        self.shared_logs_names = "@@@".join([x for x in self.shared_logs])
        return self

    def get_visualization(self, min_acti_count=0, min_paths_count=0, model_type=defaults.DEFAULT_MODEL_TYPE,
                          classifier="activity", aggregation_measure="events", projection="no",
                          decoration_measure="frequency", epsilon=0.0, noise_threshold=0.0):
        obj = copy(self)
        obj.get_names()
        obj.selected_min_acti_count = min_acti_count
        obj.selected_min_edge_freq_count = min_paths_count
        obj.selected_model_type = model_type
        obj.selected_classifier = classifier
        obj.selected_aggregation_measure = aggregation_measure
        obj.selected_projection = projection
        obj.selected_decoration_measure = decoration_measure
        obj.epsilon = epsilon
        obj.noise_threshold = noise_threshold
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

        if obj.selected_model_type.startswith("model"):
            obj.get_dfg_visualization()
        elif obj.selected_model_type.startswith("petri"):
            obj.get_petri_visualization()
        elif obj.selected_model_type.startswith("mvp"):
            obj.get_mvp_visualization()
        elif obj.selected_model_type.startswith("multigraph"):
            obj.get_multigraph_visualization()
        else:
            obj.get_new_visualization()
        return obj

    def get_multigraph_visualization(self):
        self.epsilon = float(self.epsilon)
        self.noise_threshold = float(self.noise_threshold)
        model = mdfg_disc_factory3.apply(self.succint_dataframe,
                                         parameters={"min_act_freq": self.selected_min_acti_count,
                                                     "min_edge_freq": self.selected_min_edge_freq_count,
                                                     "epsilon": self.epsilon,
                                                     "noise_obj_number": self.noise_threshold})
        gviz = mdfg_vis_factory3.apply(model, measure=self.selected_decoration_measure,
                                       freq=self.selected_aggregation_measure,
                                       projection=self.selected_projection,
                                       parameters={"format": "svg", "min_act_freq": self.selected_min_acti_count,
                                                   "min_edge_freq": self.selected_min_edge_freq_count})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def get_new_visualization(self):
        classifier_function = None
        if self.selected_classifier == "activity":
            classifier_function = lambda x: x["event_activity"]
        elif self.selected_classifier == "combined":
            classifier_function = lambda x: x["event_activity"] + "+" + x["event_objtype"]
        model = mdfg_disc_factory2.apply(self.exploded_dataframe, classifier_function=classifier_function,
                                         variant=self.selected_model_type,
                                         parameters={"min_acti_freq": self.selected_min_acti_count,
                                                     "min_edge_freq": self.selected_min_edge_freq_count})
        gviz = mdfg_vis_factory2.apply(model, measure=self.selected_decoration_measure,
                                       freq=self.selected_aggregation_measure, classifier=self.selected_classifier,
                                       projection=self.selected_projection,
                                       parameters={"format": "svg", "min_acti_freq": self.selected_min_acti_count,
                                                   "min_edge_freq": self.selected_min_edge_freq_count})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def get_dfg_visualization(self):
        if self.selected_model_type == "model1":
            model = mdfg_disc_factory.apply(self.exploded_dataframe, model_type_variant=self.selected_model_type,
                                            node_freq_variant="type1", edge_freq_variant="type11")
        elif self.selected_model_type == "model2":
            model = mdfg_disc_factory.apply(self.exploded_dataframe, model_type_variant=self.selected_model_type,
                                            node_freq_variant="type21", edge_freq_variant="type211")
        elif self.selected_model_type == "model3":
            model = mdfg_disc_factory.apply(self.exploded_dataframe, model_type_variant=self.selected_model_type,
                                            node_freq_variant="type31", edge_freq_variant="type11")
        gviz = mdfg_vis_factory.apply(model, parameters={"min_node_freq": self.selected_min_acti_count,
                                                         "min_edge_freq": self.selected_min_edge_freq_count,
                                                         "format": "svg"})
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mdfg_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def get_petri_visualization(self):
        model = petri_disc_factory.apply(self.exploded_dataframe, parameters={"min_node_freq": self.selected_min_acti_count,
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
        model = mvp_discovery.apply(self.exploded_dataframe, parameters=parameters)
        gviz = mvp_vis_factory.apply(model, parameters=parameters)
        tfilepath = tempfile.NamedTemporaryFile(suffix='.svg')
        tfilepath.close()
        mvp_vis_factory.save(gviz, tfilepath.name)
        self.model_view = base64.b64encode(open(tfilepath.name, "rb").read()).decode('utf-8')

    def apply_spec_path_filter(self, session, obytype, act1, act2, minp, maxp):
        obj = copy(self.session_objects[session])
        obj.exploded_dataframe = self.exploded_dataframe.copy()
        obj.exploded_dataframe = filter_specific_path.apply(obj.exploded_dataframe, obytype, act1, act2, minp, maxp)
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.reset_properties()
        obj.set_properties()
        self.session_objects[session] = obj

    def apply_activity_filter(self, session, activity, positive):
        obj = copy(self.session_objects[session])
        obj.exploded_dataframe = self.exploded_dataframe.copy()
        fd0 = obj.exploded_dataframe[obj.exploded_dataframe["event_activity"] == activity]
        if positive == "1":
            obj.exploded_dataframe = filter_metaclass.do_filtering(obj.exploded_dataframe, fd0)
        else:
            obj.exploded_dataframe = filter_metaclass.do_negative_filtering(obj.exploded_dataframe, fd0)
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.reset_properties()
        obj.set_properties()
        self.session_objects[session] = obj

    def apply_float_filter(self, session, activity, attr_name, v1, v2):
        obj = copy(self.session_objects[session])
        obj.exploded_dataframe = self.exploded_dataframe.copy()
        obj.exploded_dataframe = filter_act_attr_val.filter_float(obj.exploded_dataframe, activity, attr_name, v1, v2)
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.reset_properties()
        obj.set_properties()
        self.session_objects[session] = obj

    def apply_ot_filter(self, session, activity, ot, v1, v2):
        obj = copy(self.session_objects[session])
        obj.exploded_dataframe = self.exploded_dataframe.copy()
        obj.exploded_dataframe = filter_act_ot.filter_ot(obj.exploded_dataframe, activity, ot, v1, v2)
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.reset_properties()
        obj.set_properties()
        self.session_objects[session] = obj

    def apply_timestamp_filter(self, session, dt1, dt2):
        obj = copy(self.session_objects[session])
        obj.exploded_dataframe = self.exploded_dataframe.copy()
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.exploded_dataframe = filter_timestamp.apply(obj.exploded_dataframe, dt1, dt2)
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.reset_properties()
        obj.set_properties()
        self.session_objects[session] = obj

    def filter_on_cluster(self, session, cluster):
        obj = copy(self.session_objects[session])
        obj.exploded_dataframe = self.exploded_dataframe.copy()
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.exploded_dataframe = obj.exploded_dataframe[obj.exploded_dataframe["event_id"].isin(self.clusters[cluster])]
        obj.succint_dataframe = exploded_mdl_to_succint_mdl.apply(obj.exploded_dataframe)
        obj.reset_properties()
        obj.set_properties()
        self.session_objects[session] = obj

    def get_float_attr_summary(self, session, activity, attr_name):
        return distr_act_attrname.get(self.session_objects[session].exploded_dataframe, activity, attr_name)

    def get_timestamp_summary(self, session):
        return dist_timestamp.get(self.session_objects[session].exploded_dataframe)

    def get_ot_distr_summary(self, session, activity, ot):
        return distr_act_otype.get(self.session_objects[session].exploded_dataframe, activity, ot)

    def reset_filters(self, session):
        self.session_objects[session] = self.parent
        self.session_objects[session].selected_act_obj_types = deepcopy(
            self.session_objects[session].initial_act_obj_types)
        self.session_objects[session].set_properties()
