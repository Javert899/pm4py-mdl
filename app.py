from flask import Flask, render_template, request, make_response, jsonify
from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from controllers.process import Process
from controllers import defaults
import uuid

class Shared:
    logs = {}

app = Flask(__name__)


@app.route("/process_view/<process>")
def process_view(process=None):
    session = request.cookies.get('session') if 'session' in request.cookies else uuid.uuid4()
    min_acti_count = request.cookies.get('min_acti_count') if 'min_acti_count' in request.cookies else 0
    min_paths_count = request.cookies.get('min_paths_count') if 'min_paths_count' in request.cookies else 0
    model_type = request.cookies.get('model_type') if 'model_type' in request.cookies else defaults.DEFAULT_MODEL_TYPE

    min_acti_count = int(min_acti_count)
    min_paths_count = int(min_paths_count)

    response = make_response(render_template(
        'process_view.html',
        Process=Shared.logs[process].get_controller(session).get_visualization(min_acti_count=min_acti_count, min_paths_count=min_paths_count, model_type=model_type)
        ))

    if not request.cookies.get('session'):
        response.set_cookie('session', str(uuid.uuid4()))
    if not request.cookies.get('min_acti_count'):
        response.set_cookie('min_acti_count', str(min_acti_count))
    if not request.cookies.get('min_paths_count'):
        response.set_cookie('min_paths_count', str(min_paths_count))
    if not request.cookies.get('model_type'):
        response.set_cookie('model_type', model_type)

    return response


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
    Shared.logs["orders"] = Process("orders", "example_logs/mdl/order_management.mdl", Shared.logs)
    Shared.logs["orders2"] = Process("orders2", "example_logs/mdl/order_management.mdl", Shared.logs)
    app.run()


if __name__ == "__main__":
    main()
