from flask import Flask, request, jsonify
from flask_cors import CORS
from pm4pymdl.order_log_generation import generate_log
from pm4py.visualization.common.utils import get_base64_from_gviz
from pm4pymdl.algo.mvp.gen_framework import factory as gen_fram_disc_factory
from pm4pymdl.visualization.mvp.gen_framework import factory as gen_vis_factory
import base64

class Shared:
    df = generate_log()

app = Flask(__name__, static_url_path='', static_folder="webapp/dist")
app.add_url_rule(app.static_url_path + '/<path:filename>', endpoint='static',
                 view_func=app.send_static_file)
CORS(app)


@app.route("/getProcessSchema", methods=["GET"])
def get_process_schema():

    # process = request.args.get('process', default='receipt', type=str)
    # df, model_type_variant=MODEL1, rel_ev_variant=REL_DFG, node_freq_variant=TYPE1, edge_freq_variant=TYPE11, parameters=None

    model_type_variant = request.args.get('model_type_variant', default="model1", type=str)
    rel_ev_variant = request.args.get('rel_ev_variant', default="rel_dfg", type=str)
    node_freq_variant = request.args.get('node_freq_variant', default="type1", type=str)
    edge_freq_variant = request.args.get('edge_freq_variant', default="type11", type=str)

    model = gen_fram_disc_factory.apply(Shared.df, model_type_variant=model_type_variant, rel_ev_variant=rel_ev_variant,
                                        node_freq_variant=node_freq_variant, edge_freq_variant=edge_freq_variant)
    gviz = gen_vis_factory.apply(model, parameters={"format": "svg"})

    gviz_base64 = base64.b64encode(str(gviz).encode('utf-8'))

    return {"gviz": gviz_base64.decode('utf-8'), "model": get_base64_from_gviz(gviz).decode('utf-8')}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5000", threaded=True)
