from pm4pymdl.algo.mvp.gen_framework.edge_freq import type11, type12, type13, type211, type212, type221, type222, type231, type232

TYPE11 = "type11"
TYPE12 = "type12"
TYPE13 = "type13"
TYPE211 = "type211"
TYPE212 = "type212"
TYPE221 = "type221"
TYPE222 = "type222"
TYPE231 = "type231"
TYPE232 = "type232"

VERSIONS = {TYPE11: type11.apply, TYPE12: type12.apply, TYPE13: type13.apply, TYPE211: type211.apply,
            TYPE212: type212.apply, TYPE221: type221.apply, TYPE222: type222.apply, TYPE231: type231.apply,
            TYPE232: type232.apply}


def apply(df, model, rel_ev, rel_act, variant=TYPE11, parameters=None):
    return VERSIONS[variant](df, model, rel_ev, rel_act, parameters=parameters)
