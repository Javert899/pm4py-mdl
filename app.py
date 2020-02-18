from flask import Flask, render_template
from pm4pymdl.objects.mdl.importer import factory as mdl_importer
from controllers.process import Process

class Shared:
    logs = {}

app = Flask(__name__)


@app.route("/process_view/<process>")
def process_view(process=None):
    return render_template(
        'process_view.html',
        Process=Shared.logs[process]
        )


def main():
    Shared.logs["orders"] = Process("orders", "example_logs/mdl/order_management.mdl")
    app.run()


if __name__ == "__main__":
    main()
