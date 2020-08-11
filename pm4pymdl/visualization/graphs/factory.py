import os
import shutil
import subprocess
import sys
import traceback
from graphviz import Digraph
import uuid
import tempfile


def apply(graph, parameters=None):
    if parameters is None:
        parameters = {}

    image_format = "png"
    if "format" in parameters:
        image_format = parameters["format"]




def view(figure):
    """
    View on the screen a figure that has been rendered

    Parameters
    ----------
    figure
        figure
    """
    try:
        filename = figure.name
        figure = filename
    except AttributeError:
        # continue without problems, a proper path has been provided
        pass

    is_ipynb = False

    try:
        get_ipython()
        is_ipynb = True
    except NameError:
        pass

    if is_ipynb:
        from IPython.display import Image
        return Image(open(figure, "rb").read())
    else:
        if sys.platform.startswith('darwin'):
            subprocess.call(('open', figure))
        elif os.name == 'nt':  # For Windows
            os.startfile(figure)
        elif os.name == 'posix':  # For Linux, Mac, etc.
            subprocess.call(('xdg-open', figure))


def save(figure, output_file_path):
    """
    Save a figure that has been rendered

    Parameters
    -----------
    figure
        figure
    output_file_path
        Path where the figure should be saved
    """
    try:
        filename = figure.name
        figure = filename
    except AttributeError:
        # continue without problems, a proper path has been provided
        pass
        traceback.print_exc()

    shutil.copyfile(figure, output_file_path)
