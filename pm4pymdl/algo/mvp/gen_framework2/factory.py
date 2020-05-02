from pm4pymdl.algo.mvp.gen_framework2 import md_petri_inductive, md_petri_alpha, md_process_trees


def apply(df, variant="petri_inductive", classifier_function=None, parameters=None):
    if parameters is None:
        parameters = {}

    if variant == "petri_inductive":
        return md_petri_inductive.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
    elif variant == "petri_alpha":
        return md_petri_alpha.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
    elif variant == "process_tree":
        return md_process_trees.discovery.apply(df, classifier_function=classifier_function, parameters=parameters)
