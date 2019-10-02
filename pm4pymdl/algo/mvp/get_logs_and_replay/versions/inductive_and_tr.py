import pm4py
from pm4pymdl.algo.mvp.utils import succint_mdl_to_exploded_mdl
from pm4pymdl.algo.mvp.projection import factory
from pm4py.algo.discovery.inductive import factory as inductive_miner
from pm4py.algo.conformance.tokenreplay import factory as tr_factory
from pm4py.visualization.petrinet.util import performance_map
from pm4py.algo.filtering.log.variants import variants_filter as variants_module

PARAM_ACTIVITY_KEY = pm4py.util.constants.PARAMETER_CONSTANT_ACTIVITY_KEY

def apply(df, parameters=None):
    if df.type == "succint":
        df = succint_mdl_to_exploded_mdl(df)
        df.type = "exploded"

    persps = [x for x in df.columns if not x.startswith("event_")]

    ret = {}
    ret["nets"] = {}
    ret["act_count"] = {}
    ret["replay"] = {}
    ret["group_size_hist"] = {}
    ret["act_count_replay"] = {}
    ret["group_size_hist_replay"] = {}

    for persp in persps:

        log = factory.apply(df, persp, parameters=parameters)

        net, im, fm = inductive_miner.apply(log)
        activ_count = factory.apply(df, persp, variant="activity_occurrence", parameters=parameters)

        variants_idx = variants_module.get_variants_from_log_trace_idx(log)
        variants = variants_module.convert_variants_trace_idx_to_trace_obj(log, variants_idx)
        parameters_tr = {PARAM_ACTIVITY_KEY: "concept:name", "variants": variants}

        aligned_traces, place_fitness_per_trace, transition_fitness_per_trace, notexisting_activities_in_model = tr_factory.apply(log, net, im, fm, parameters={"enable_place_fitness": True, "disable_variants": True})

        element_statistics = performance_map.single_element_statistics(log, net, im,
                                                                       aligned_traces, variants_idx)

        aggregated_statistics = performance_map.aggregate_statistics(element_statistics)

        group_size_hist = factory.apply(df, persp, variant="group_size_hist", parameters=parameters)

        occurrences = {}
        for trans in transition_fitness_per_trace:
            occurrences[trans.label] = []
            for trace in transition_fitness_per_trace[trans]["fit_traces"]:
                if not trace in transition_fitness_per_trace[trans]["underfed_traces"]:
                    case_id = trace.attributes["concept:name"]
                    for event in trace:
                        if event["concept:name"] == trans.label:
                            occurrences[trans.label].append([case_id, event["event_id"]])
            #print(transition_fitness_per_trace[trans])


        len_different_ids = {}
        for act in occurrences:
            len_different_ids[act] = len(set(x[1] for x in occurrences[act]))

        eid_acti_count = {}
        for act in occurrences:
            eid_acti_count[act] = {}
            for x in occurrences[act]:
                if not x[0] in eid_acti_count:
                    eid_acti_count[act][x[0]] = 0
                eid_acti_count[act][x[0]] = eid_acti_count[act][x[0]] + 1
            eid_acti_count[act] = sorted(list(eid_acti_count[act].values()))

        ret["nets"][persp] = [net, im, fm]
        ret["act_count"][persp] = activ_count
        ret["replay"][persp] = aggregated_statistics
        ret["group_size_hist"][persp] = group_size_hist
        ret["act_count_replay"][persp] = len_different_ids
        ret["group_size_hist_replay"][persp] = eid_acti_count

        #print(ret["act_count"])
        #print(ret["act_count_replay"])


    return ret
