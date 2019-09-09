class BasicModelStructure(object):
    def __init__(self, df, parameters=None):
        if parameters is None:
            parameters = {}

        self.df = df
        self.rel_ev = None
        self.rel_act = None
        self.node_freq = None
        self.edge_freq = None

    def get_rel_ev(self):
        return self.rel_ev

    def set_rel_ev(self, rel_ev):
        self.rel_ev = rel_ev

    def get_rel_act(self):
        return self.rel_act

    def set_rel_act(self, rel_act):
        self.rel_act = rel_act

    def get_node_freq(self):
        return self.node_freq

    def set_node_freq(self, node_freq):
        self.node_freq = node_freq

    def get_edge_freq(self):
        return self.edge_freq

    def set_edge_freq(self, edge_freq):
        self.edge_freq = edge_freq

    def get_perspectives(self):
        if self.rel_ev is not None:
            return set(self.rel_ev.keys())
        return set()

    def get_all_activities(self):
        all_activities = set()
        if self.node_freq is not None:
            for el in self.node_freq:
                if type(self.node_freq[el]) is str:
                    all_activities = all_activities.union(self.node_freq[el])
                else:
                    all_activities = set(self.node_freq.keys())
        return all_activities

