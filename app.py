from flask import Flask, render_template, request, make_response
from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from controllers.process import Process
import uuid

class Shared:
    logs = {}

app = Flask(__name__)


@app.route("/process_view/<process>")
def process_view(process=None):
    session = request.cookies.get('session') if 'session' in request.cookies else uuid.uuid4()
    response = make_response(render_template(
        'process_view.html',
        Process=Shared.logs[process].get_controller(session)
        ))
    if not request.cookies.get('session'):
        response.set_cookie('session', str(uuid.uuid4()))
    return response


def main():
    Shared.logs["orders"] = Process("orders", "example_logs/mdl/order_management.mdl")
    app.run()


if __name__ == "__main__":
    main()
