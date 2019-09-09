from pm4pymdl.algo.mvp.gen_framework.rel_activities import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(df, model, rel_ev_dict, variant=CLASSIC, parameters=None):
    if parameters is None:
        parameters = {}

    return VERSIONS[variant](df, model, rel_ev_dict, parameters=parameters)
