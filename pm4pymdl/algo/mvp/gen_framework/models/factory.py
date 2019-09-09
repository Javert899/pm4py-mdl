from pm4pymdl.algo.mvp.gen_framework.models import model1
from pm4pymdl.algo.mvp.gen_framework.models import model2
from pm4pymdl.algo.mvp.gen_framework.models import model3


MODEL1 = "model1"
MODEL2 = "model2"
MODEL3 = "model3"

VERSIONS = {MODEL1: model1.MVPModel1, MODEL2: model2.MVPModel2, MODEL3: model3.MVPModel3}


def apply(df, variant=MODEL1, parameters=None):
    model = VERSIONS[variant](df, parameters=parameters)
    if variant == MODEL2:
        model.calculate_map()
    return model

