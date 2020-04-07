from flask import Flask, render_template, request, make_response, jsonify
import base64
from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from controllers import process as pc_controller
from controllers.process import Process
from controllers import defaults
import uuid
import json

class Shared:
    logs = {}

app = Flask(__name__)


@app.route("/welcome")
def welcome():
    session = request.cookies.get('session') if 'session' in request.cookies else str(uuid.uuid4())
    response = make_response(render_template(
        'welcome.html',
        Process=Shared.logs[list(Shared.logs.keys())[0]].get_names()
        ))
    if not request.cookies.get('session'):
        response.set_cookie('session', session)
    return response


@app.route("/act_ot_selection/<process>")
def act_ot_selection(process=None):
    session = request.cookies.get('session') if 'session' in request.cookies else str(uuid.uuid4())
    response = make_response(render_template(
        'act_ot_selection.html',
        Process=Shared.logs[process].get_controller(session)
        ))
    if not request.cookies.get('session'):
        response.set_cookie('session', session)
    return response


@app.route("/cases_view/<process>")
def cases_view(process=None):
    session = request.cookies.get('session') if 'session' in request.cookies else str(uuid.uuid4())
    if 'exponent' in request.cookies:
        pc_controller.DEFAULT_EXPONENT = int(request.cookies['exponent'])
    response = make_response(render_template(
        'cases_view.html',
        Process=Shared.logs[process].get_controller(session).events_list()
        ))
    if not request.cookies.get('session'):
        response.set_cookie('session', session)
    if not request.cookies.get('exponent'):
        response.set_cookie('exponent', str(pc_controller.DEFAULT_EXPONENT))
    return response


@app.route("/process_view/<process>")
def process_view(process=None):
    session = request.cookies.get('session') if 'session' in request.cookies else str(uuid.uuid4())
    process = Shared.logs[process].get_controller(session)
    min_acti_count = request.cookies.get('min_acti_count') if 'min_acti_count' in request.cookies else process.selected_min_acti_count
    min_paths_count = request.cookies.get('min_paths_count') if 'min_paths_count' in request.cookies else process.selected_min_edge_freq_count
    model_type = request.cookies.get('model_type') if 'model_type' in request.cookies else defaults.DEFAULT_MODEL_TYPE
    if 'exponent' in request.cookies:
        pc_controller.DEFAULT_EXPONENT = int(request.cookies['exponent'])
    min_acti_count = int(min_acti_count)
    min_paths_count = int(min_paths_count)
    response = make_response(render_template(
        'process_view.html',
        Process=process.get_visualization(min_acti_count=min_acti_count, min_paths_count=min_paths_count, model_type=model_type)
        ))

    if not request.cookies.get('session'):
        response.set_cookie('session', session)
    if not request.cookies.get('min_acti_count'):
        response.set_cookie('min_acti_count', str(min_acti_count))
    if not request.cookies.get('min_paths_count'):
        response.set_cookie('min_paths_count', str(min_paths_count))
    if not request.cookies.get('model_type'):
        response.set_cookie('model_type', model_type)
    if not request.cookies.get('exponent'):
        response.set_cookie('exponent', str(pc_controller.DEFAULT_EXPONENT))
    return response


@app.route("/saveActOtSelection/")
def save_act_ot_selection():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    actotselection = json.loads(base64.b64decode(request.args.get('actotselection')).decode('utf-8'))

    process = Shared.logs[process].get_controller(session)
    process.selected_act_obj_types = actotselection

    return ""


@app.route("/getMostSimilar")
def get_most_similar():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    eid = request.args.get('eid')
    exponent = int(request.cookies.get('exponent')) if 'exponent' in request.cookies else pc_controller.DEFAULT_EXPONENT

    process = Shared.logs[process].get_controller(session)

    return jsonify(process.get_most_similar(eid, exponent=exponent))


@app.route("/getLogObjectType")
def get_log_object_type():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    objtype = request.args.get('objtype')

    process = Shared.logs[process].get_controller(session)
    name, type, log = process.get_log_obj_type(objtype)

    return jsonify({"name": name, "type": type, "log": log})

@app.route("/getSpecObjTable")
def get_spec_obj_table():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    objtype = request.args.get('objtype')
    objid = request.args.get('objid')

    process = Shared.logs[process].get_controller(session)

    return jsonify(process.events_list_spec_objt(objid, objtype))


@app.route("/applySpecPathFilter")
def apply_spec_path_filter():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    objtype = request.args.get('objtype')
    act1 = request.args.get('act1')
    act2 = request.args.get('act2')

    process = Shared.logs[process].get_controller(session)
    process.apply_spec_path_filter(session, objtype, act1, act2)

    return ""


@app.route("/applyClusterFilter")
def apply_cluster_filter():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    cluster = request.args.get('cluster')

    process = Shared.logs[process].get_controller(session)
    process.filter_on_cluster(session, cluster)

    return ""

@app.route("/applyActivityFilter")
def apply_activity_filter():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    activity = request.args.get('activity')
    positive = request.args.get('positive')

    process = Shared.logs[process].get_controller(session)
    process.apply_activity_filter(session, activity, positive)

    return ""


@app.route("/applyFloatFilter")
def apply_float_filter():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    activity = request.args.get('activity')
    attr_name = request.args.get('attr_name')
    min_value = float(request.args.get('min_value'))
    max_value = float(request.args.get('max_value'))
    process = request.args.get('process')

    process = Shared.logs[process].get_controller(session)
    process.apply_float_filter(session, activity, attr_name, min_value, max_value)

    return ""


@app.route("/applyTimestampFilter")
def apply_timestamp_filter():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')
    min_value = request.args.get('min_value')
    max_value = request.args.get('max_value')

    process = Shared.logs[process].get_controller(session)
    process.apply_timestamp_filter(session, min_value, max_value)

    return ""


@app.route("/getTimestampDistribution")
def get_timestamp_distribution():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')

    process = Shared.logs[process].get_controller(session)
    ret = process.get_timestamp_summary(session)

    return jsonify(ret)


@app.route("/applyObjTypesFilter")
def apply_obj_types_filter():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    activity = request.args.get('activity')
    ot = request.args.get('ot')
    min_value = float(request.args.get('min_value'))
    max_value = float(request.args.get('max_value'))
    process = request.args.get('process')

    process = Shared.logs[process].get_controller(session)
    process.apply_ot_filter(session, activity, ot, min_value, max_value)

    return ""


@app.route("/getFloatAttrSummary")
def get_float_attr_summary():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    activity = request.args.get('activity')
    attr_name = request.args.get('attr_name')
    process = request.args.get('process')

    process = Shared.logs[process].get_controller(session)

    return jsonify(process.get_float_attr_summary(session, activity, attr_name))


@app.route("/getOtDistrSummary")
def get_ot_distr_summary():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    activity = request.args.get('activity')
    ot = request.args.get('ot')
    process = request.args.get('process')

    process = Shared.logs[process].get_controller(session)

    dictio = process.get_ot_distr_summary(session, activity, ot)

    new_dictio = {}
    for x,y in dictio.items():
        if "int" in str(type(x)):
            x = int(x)
        elif "float" in str(type(x)):
            x = float(x)
        if "int" in str(type(y)):
            y = int(y)
        elif "float" in str(type(y)):
            y = float(x)
        new_dictio[x] = y
    #dictio = {x: float(y) for x, y in dictio.items() if "int" in str(type(y)) or "float" in str(type(y))}

    return jsonify(new_dictio)


@app.route("/resetFilters")
def reset_filters():
    session = request.cookies.get('session')
    if session is None:
        raise Exception()
    process = request.args.get('process')

    Shared.logs[process].reset_filters(session)

    return ""


def main():
    if defaults.CONFIGURATION == 1:
        Shared.logs["orders"] = Process("orders", "example_logs/mdl/order_management.mdl", Shared.logs)
        Shared.logs["orders2"] = Process("orders2", "example_logs/mdl/order_management.mdl", Shared.logs)
    elif defaults.CONFIGURATION == 2:
        Shared.logs["bkpf"] = Process("bkpf", "sap/bkpf_bseg.mdl", Shared.logs)
        Shared.logs["cdhdr"] = Process("cdhdr", "sap/sap_withTrial.mdl", Shared.logs)
        Shared.logs["opportunities"] = Process("opportunities", "example_logs/mdl/log_opp_red.mdl", Shared.logs)
    elif defaults.CONFIGURATION == 3:
        Shared.logs["runningexample"] = Process("runningexample", "example_logs/mdl/mdl-running-example.mdl", Shared.logs)
        Shared.logs["orders"] = Process("orders", "example_logs/mdl/order_management.mdl", Shared.logs)
        Shared.logs["runningexample"].initial_act_obj_types = {"place order": ["orders", "items"],
                                                               "confirm order": ["orders", "items"],
                                                               "pay order": ["orders"],
                                                               "payment reminder": ["orders"],
                                                               "create package": ["packages", "itemss"],
                                                               "send package": ["packages", "items"],
                                                               "failed delivery": ["packages", "items"],
                                                               "package delivered": ["packages", "items"],
                                                               "pick item": ["items"],
                                                               "reorder item": ["items"],
                                                               "item out of stock": ["items"]}
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()
