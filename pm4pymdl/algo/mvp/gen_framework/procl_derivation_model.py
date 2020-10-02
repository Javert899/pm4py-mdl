from pm4pymdl.algo.mvp.gen_framework import algorithm as gen_fram_factory
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.dfg.utils import dfg_utils


class ProclDerivationModel(object):
    def __init__(self, df, parameters=None):
        if parameters is None:
            parameters = {}

        self.type = "proclet_derivation_model"

        self.df = df
        self.model_dfg = gen_fram_factory.apply(df, model_type_variant="model3", rel_ev_variant="rel_dfg",
                                                node_freq_variant="type31", edge_freq_variant="type11")
        self.model_3_1 = gen_fram_factory.apply(df, model_type_variant="model3", rel_ev_variant="being_produced",
                                                node_freq_variant="type31", edge_freq_variant="type11")
        self.model_3_2 = gen_fram_factory.apply(df, model_type_variant="model3", rel_ev_variant="being_produced",
                                                node_freq_variant="type32", edge_freq_variant="type13")
        self.model_link = gen_fram_factory.apply(df, model_type_variant="model3", rel_ev_variant="link",
                                                 node_freq_variant="type32", edge_freq_variant="type11")

        self.model_inductive = {}
        self.start_activities = {}
        self.end_activities = {}

        for cl in self.model_dfg.edge_freq:
            this_dfg = self.model_dfg.edge_freq[cl]
            new_dfg = {(x.split("@@")[0], x.split("@@")[1]): this_dfg[x] for x in this_dfg}
            self.model_inductive[cl] = inductive_miner.apply_dfg(new_dfg)
            self.start_activities[cl] = dfg_utils.infer_start_activities(new_dfg)
            self.end_activities[cl] = dfg_utils.infer_end_activities(new_dfg)

        self.linked_perspectives = {}

        self.find_linked_perspectives()

    def find_linked_perspectives(self):
        self.act_count_1 = self.model_3_1.node_freq
        self.act_count_2 = self.model_3_2.node_freq

        self.acti_class_map = {}

        for cl in self.act_count_1:
            for act in self.act_count_1[cl]:
                if not act in self.acti_class_map:
                    self.acti_class_map[act] = set()
                self.acti_class_map[act].add(cl)

        self.loop_count_1 = {}
        self.loop_count_2 = {}
        self.acti_loop_map = {}
        self.production_map = {}

        for cl in self.model_3_1.edge_freq:
            self.loop_count_1[cl] = {}
            for edge in self.model_3_1.edge_freq[cl]:
                acti = edge.split("@@")[0]
                self.loop_count_1[cl][acti] = self.model_3_1.edge_freq[cl][edge]
                if not acti in self.acti_loop_map:
                    self.acti_loop_map[acti] = set()
                self.acti_loop_map[acti].add(cl)

        for cl in self.model_3_2.edge_freq:
            self.loop_count_2[cl] = {}
            for edge in self.model_3_2.edge_freq[cl]:
                self.loop_count_2[cl][edge.split("@@")[0]] = self.model_3_2.edge_freq[cl][edge]

        for acti in self.acti_loop_map:
            if len(self.acti_loop_map[acti]) == 1 and len(self.acti_class_map[acti]) > 1:
                current_class = list(self.acti_loop_map[acti])[0]
                remaining_classes = self.acti_class_map[acti] - self.acti_loop_map[acti]
                linked_acti_current = set(
                    (x,
                     self.model_3_2.node_freq[current_class][x.split("@@")[1]],
                     self.model_3_2.node_freq[current_class][x.split("@@")[0]]) for x in
                    self.model_link.edge_freq[current_class] if
                    x.split("@@")[0] == acti and self.model_link.edge_freq[current_class][x] >=
                    self.model_3_1.node_freq[current_class][x.split("@@")[1]])

                if linked_acti_current:
                    prod_count_1_source = self.loop_count_1[current_class][acti]
                    acti_count_1_source = self.act_count_1[current_class][acti]
                    prod_count_2_source = self.loop_count_2[current_class][acti]

                    if prod_count_1_source == acti_count_1_source:
                        if prod_count_2_source > prod_count_1_source:
                            source_card = "N"
                        else:
                            source_card = "1"
                    else:
                        if prod_count_2_source > prod_count_1_source:
                            source_card = "N"
                        else:
                            source_card = "1"

                    for cl in remaining_classes:
                        linked = None
                        linked = []

                        for x in linked_acti_current:
                            ta = x[0].split("@@")[1]
                            if ta in self.act_count_1[cl]:
                                if x[1] == x[2]:
                                    linked.append([x[0].split("@@")[1], "="])
                                else:
                                    linked.append([x[0].split("@@")[1], "<="])

                        if linked:
                            if current_class not in self.production_map:
                                self.production_map[current_class] = {}
                            if cl not in self.production_map[current_class]:
                                self.production_map[current_class][cl] = {}

                            acti_count_1_target = self.act_count_1[cl][acti]
                            acti_count_2_target = self.act_count_2[cl][acti]

                            if acti_count_2_target > acti_count_1_target:
                                target_card = "N"
                            else:
                                target_card = "1"

                            self.production_map[current_class][cl][acti] = {"linked": linked, "source_card": source_card,
                                                                            "target_card": target_card}
