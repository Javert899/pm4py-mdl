from pm4pymdl.algo.mvp.gen_framework.models.basic_model_structure import BasicModelStructure


class MVPModel1(BasicModelStructure):
    def __init__(self, df, parameters=None):
        self.type = "model1"
        BasicModelStructure.__init__(self, df, parameters=parameters)
