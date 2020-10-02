from pm4pymdl.algo.mvp.gen_framework2 import md_petri_inductive, md_petri_alpha, md_process_trees, md_dfg, md_ts


def apply(df, variant="dfg", classifier_function=None, parameters=None):
    if parameters is None:
        parameters = {}

    if variant == "inductive_petri":
        return md_petri_inductive.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
    elif variant == "alpha_petri":
        return md_petri_alpha.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
    elif variant == "process_tree":
        return md_process_trees.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
    elif variant == "dfg":
        return md_dfg.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
    elif variant == "trans_system":
        return md_ts.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
