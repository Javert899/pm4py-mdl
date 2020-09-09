from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
import tempfile
import graphviz

def apply(model, measure="frequency", freq="events", classifier="activity", projection="no", parameters=None):
    if parameters is None:
        parameters = {}

