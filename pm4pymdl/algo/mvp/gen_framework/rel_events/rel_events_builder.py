from pm4pymdl.algo.mvp.gen_framework.rel_events import rel_dfg
from pm4pymdl.algo.mvp.gen_framework.rel_events import being_produced
from pm4pymdl.algo.mvp.gen_framework.rel_events import existence
from pm4pymdl.algo.mvp.gen_framework.rel_events import link
from pm4pymdl.algo.mvp.gen_framework.rel_events import eventually_follows

REL_DFG = "rel_dfg"
BEING_PRODUCED = "being_produced"
EXISTENCE = "existence"
LINK = "link"
EVENTUALLY_FOLLOWS = "eventually_follows"

VERSIONS = {REL_DFG: rel_dfg.apply, BEING_PRODUCED: being_produced.apply, EXISTENCE: existence.apply, LINK: link.apply,
            EVENTUALLY_FOLLOWS: eventually_follows.apply}


def apply(df, model, variant=REL_DFG, parameters=None):
    if parameters is None:
        parameters = {}

    return VERSIONS[variant](df, model, parameters=parameters)
