from pm4pymdl.algo.mvp.gen_framework.models.basic_model_structure import BasicModelStructure


class MVPModel2(BasicModelStructure):
    def __init__(self, df, parameters=None):
        self.map = None
        self.type = "model2"
        BasicModelStructure.__init__(self, df, parameters=parameters)

    def get_map(self):
        return self.map

    def set_map(self, map):
        self.map = map

    def calculate_map(self):
        self.map = {}
        activities = self.df["event_activity"].unique()
        nnc = self.df.count()

        for act in activities:
            red_df = self.df[self.df["event_activity"] == act]

            non_null_count = dict(red_df.count())

            max_key = None
            max_value = -1

            keys = sorted([key for key in non_null_count.keys() if not key.startswith("event_")])

            for key in keys:
                if non_null_count[key] > max_value:
                    max_key = key
                    max_value = non_null_count[key]
                elif non_null_count[key] >= max_value and nnc[key] > nnc[max_key]:
                    max_key = key
                    max_value = non_null_count[key]

            self.map[act] = max_key
